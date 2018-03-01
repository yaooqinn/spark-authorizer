/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.util.{ArrayList => JAList, List => JList}

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.{HivePrivilegeObjectType, HivePrivObjectActionType}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.HivePrivilegeObjectHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

/**
 * [[LogicalPlan]] -> list of [[HivePrivilegeObject]]s
 */
private[sql] object HivePrivObjsFromPlan {

  def build(
      logicalPlan: LogicalPlan,
      currentDb: String): (JList[HivePrivilegeObject], JList[HivePrivilegeObject]) = {
    val inputObjs = new JAList[HivePrivilegeObject]
    val outputObjs = new JAList[HivePrivilegeObject]
    logicalPlan match {
      // CreateTable / RunnableCommand
      case cmd: Command => buildBinaryHivePrivObject(cmd, currentDb, inputObjs, outputObjs)
      case iit: InsertIntoTable => buildBinaryHivePrivObject(iit, currentDb, inputObjs, outputObjs)
      case ct: CreateTable => buildBinaryHivePrivObject(ct, currentDb, inputObjs, outputObjs)
      case _ => buildUnaryHivePrivObjs(logicalPlan, currentDb, inputObjs)
    }
    (inputObjs, outputObjs)
  }

  /**
   * Build HivePrivilegeObjects from Spark LogicalPlan
   * @param logicalPlan a Spark LogicalPlan used to generate HivePrivilegeObjects
   * @param hivePrivilegeObjects input or output hive privilege object list
   * @param hivePrivObjType Hive Privilege Object Type
   * @param projectionList Projection list after pruning
   */
  private def buildUnaryHivePrivObjs(
      logicalPlan: LogicalPlan,
      currentDb: String,
      hivePrivilegeObjects: JList[HivePrivilegeObject],
      hivePrivObjType: HivePrivilegeObjectType = HivePrivilegeObjectType.TABLE_OR_VIEW,
      projectionList: Seq[NamedExpression] = null): Unit = {

    /**
     * Columns in Projection take priority for column level privilege checking
     * @param table catalogTable of a given relation
     */
    def handleProjectionForRelation(table: CatalogTable): Unit = {
      if (projectionList == null) {
        addTableOrViewLevelObjs(
          table.identifier,
          hivePrivilegeObjects,
          currentDb,
          table.partitionColumnNames.asJava,
          table.schema.fieldNames.toList.asJava)
      } else if (projectionList.isEmpty) {
        addTableOrViewLevelObjs(table.identifier, hivePrivilegeObjects, currentDb)
      } else {
        addTableOrViewLevelObjs(
          table.identifier,
          hivePrivilegeObjects,
          currentDb,
          table.partitionColumnNames.filter(projectionList.map(_.name).contains(_)).asJava,
          projectionList.map(_.name).asJava)
      }
    }

    logicalPlan match {
      case Project(projList, child) =>
        buildUnaryHivePrivObjs(
          child,
          currentDb,
          hivePrivilegeObjects,
          HivePrivilegeObjectType.TABLE_OR_VIEW,
          projList)
      case HiveTableRelation(tableMeta, _, _) =>
        handleProjectionForRelation(tableMeta)
      case _: InMemoryRelation =>
        // TODO: should take case of its child SparkPlan's underlying relation

      case LogicalRelation(_, _, Some(table), _) =>
        handleProjectionForRelation(table)

      case UnresolvedRelation(tableIdentifier) =>
        // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
        // Unfortunately, the real world is always a place where miracles happen.
        // We check the privileges directly without resolving the plan and leave everything
        // to spark to do.
        addTableOrViewLevelObjs(tableIdentifier, hivePrivilegeObjects, currentDb)

      case UnresolvedCatalogRelation(tableMeta) =>
        handleProjectionForRelation(tableMeta)

      case View(_, output, child) =>
        buildUnaryHivePrivObjs(child, currentDb, hivePrivilegeObjects, hivePrivObjType, output)

      case WriteToDataSourceV2(_, child) =>
        buildUnaryHivePrivObjs(
          child, currentDb, hivePrivilegeObjects, hivePrivObjType, projectionList)

      case bn: BinaryNode =>
        buildUnaryHivePrivObjs(
          bn.left, currentDb, hivePrivilegeObjects, hivePrivObjType, projectionList)
        buildUnaryHivePrivObjs(
          bn.right, currentDb, hivePrivilegeObjects, hivePrivObjType, projectionList)

      case un: UnaryNode =>
        buildUnaryHivePrivObjs(
          un.child, currentDb, hivePrivilegeObjects, hivePrivObjType, projectionList)

      case Union(children) =>
        for (child <- children) {
          buildUnaryHivePrivObjs(
            child, currentDb, hivePrivilegeObjects, hivePrivObjType, projectionList)
        }

      case _ =>
    }
  }

  /**
   * Build HivePrivilegeObjects from Spark LogicalPlan
   * @param logicalPlan a Spark LogicalPlan used to generate HivePrivilegeObjects
   * @param inputObjs input hive privilege object list
   * @param outputObjs output hive privilege object list
   */
  private def buildBinaryHivePrivObject(
      logicalPlan: LogicalPlan,
      currentDb: String,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject]): Unit = {
    logicalPlan match {
      case CreateTable(tableDesc, mode, maybePlan) =>
        addDbLevelObjs(tableDesc.identifier, outputObjs, currentDb)
        addTableOrViewLevelObjs(tableDesc.identifier, outputObjs, currentDb, mode = mode)
        maybePlan.foreach {
          buildUnaryHivePrivObjs(_, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
        }

      case InsertIntoTable(table, _, child, _, _) =>
        // table is a logical plan not catalogTable, so miss overwrite and partition info.
        // TODO: deal with overwrite
        buildUnaryHivePrivObjs(table, currentDb, outputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
        buildUnaryHivePrivObjs(child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case a: AlterDatabasePropertiesCommand => addDbLevelObjs(a.databaseName, outputObjs)

      case a: AlterTableAddColumnsCommand =>
        addTableOrViewLevelObjs(
          a.table, inputObjs, currentDb, columns = a.colsToAdd.map(_.name).toList.asJava)
        addTableOrViewLevelObjs(
          a.table, outputObjs, currentDb, columns = a.colsToAdd.map(_.name).toList.asJava)

      case a: AlterTableAddPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableChangeColumnCommand =>
        addTableOrViewLevelObjs(
          a.tableName, inputObjs, currentDb, columns = Seq(a.columnName).toList.asJava)

      case a: AlterTableDropPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableRecoverPartitionsCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableRenameCommand if !a.isView || a.oldName.database.nonEmpty =>
        // rename tables / permanent views
        addTableOrViewLevelObjs(a.oldName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.newName, outputObjs, currentDb)

      case a: AlterTableRenamePartitionCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableSerDePropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableSetLocationCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableSetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterTableUnsetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName, inputObjs, currentDb)
        addTableOrViewLevelObjs(a.tableName, outputObjs, currentDb)

      case a: AlterViewAsCommand =>
        if (a.name.database.nonEmpty) {
          // it's a permanent view
          addTableOrViewLevelObjs(a.name, outputObjs, currentDb)
        }
        buildUnaryHivePrivObjs(
          a.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case a: AnalyzeColumnCommand =>
        addTableOrViewLevelObjs(
          a.tableIdent, inputObjs, currentDb, columns = a.columnNames.toList.asJava)
        addTableOrViewLevelObjs(
          a.tableIdent, outputObjs, currentDb, columns = a.columnNames.toList.asJava)

      case a: AnalyzePartitionCommand =>
        addTableOrViewLevelObjs(
          a.tableIdent, inputObjs, currentDb)
        addTableOrViewLevelObjs(
          a.tableIdent, outputObjs, currentDb)

      case a: AnalyzeTableCommand =>
        val columns = new JAList[String]()
        columns.add("RAW__DATA__SIZE")
        addTableOrViewLevelObjs(a.tableIdent, inputObjs, currentDb, columns = columns)
        addTableOrViewLevelObjs(a.tableIdent, outputObjs, currentDb)

      case c: CacheTableCommand =>
        c.plan.foreach {
          buildUnaryHivePrivObjs(_, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
        }

      case c: CreateDatabaseCommand =>
        addDbLevelObjs(c.databaseName, outputObjs)

      case c: CreateDataSourceTableAsSelectCommand =>
        addDbLevelObjs(c.table.identifier, outputObjs, currentDb)
        addTableOrViewLevelObjs(c.table.identifier, outputObjs, currentDb, mode = c.mode)
        buildUnaryHivePrivObjs(
          c.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case c: CreateDataSourceTableCommand =>
        addTableOrViewLevelObjs(c.table.identifier, outputObjs, currentDb)

      case c: CreateFunctionCommand if !c.isTemp =>
        addDbLevelObjs(c.databaseName, outputObjs, currentDb)
        addFunctionLevelObjs(c.databaseName, c.functionName, outputObjs, currentDb)

      case c: CreateHiveTableAsSelectCommand =>
        addDbLevelObjs(c.tableDesc.identifier, outputObjs, currentDb)
        addTableOrViewLevelObjs(c.tableDesc.identifier, outputObjs, currentDb)
        buildUnaryHivePrivObjs(
          c.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case c: CreateTableCommand =>
        addTableOrViewLevelObjs(c.table.identifier, outputObjs, currentDb)

      case c: CreateTableLikeCommand =>
        addDbLevelObjs(c.targetTable, outputObjs, currentDb)
        addTableOrViewLevelObjs(c.targetTable, outputObjs, currentDb)
        // hive don't handle source table's privileges, we should not obey that, because
        // it will cause meta information leak
        addDbLevelObjs(c.sourceTable, inputObjs, currentDb)
        addTableOrViewLevelObjs(c.sourceTable, inputObjs, currentDb)

      case c: CreateViewCommand =>
        c.viewType match {
          case PersistedView =>
            // PersistedView will be tied to a database
            addDbLevelObjs(c.name, outputObjs, currentDb)
            addTableOrViewLevelObjs(c.name, outputObjs, currentDb)
          case _ =>
        }
        buildUnaryHivePrivObjs(
          c.child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case d: DescribeColumnCommand =>
        addTableOrViewLevelObjs(
          d.table, inputObjs, currentDb, columns = d.colNameParts.toList.asJava)

      case d: DescribeDatabaseCommand =>
        addDbLevelObjs(d.databaseName, inputObjs)

      case d: DescribeFunctionCommand =>
        addFunctionLevelObjs(
          d.functionName.database, d.functionName.funcName, inputObjs, currentDb)

      case d: DescribeTableCommand =>
        addTableOrViewLevelObjs(d.table, inputObjs, currentDb)

      case d: DropDatabaseCommand =>
        // outputObjs are enough for privilege check, adding inputObjs for consistency with hive
        // behaviour in case of some unexpected issues.
        addDbLevelObjs(d.databaseName, inputObjs)
        addDbLevelObjs(d.databaseName, outputObjs)

      case d: DropFunctionCommand =>
        addFunctionLevelObjs(d.databaseName, d.functionName, outputObjs, currentDb)

      case d: DropTableCommand =>
        addTableOrViewLevelObjs(d.tableName, outputObjs, currentDb)

      case e: ExplainCommand =>
        buildBinaryHivePrivObject(e.logicalPlan, currentDb, inputObjs, outputObjs)

      case i: InsertIntoDataSourceCommand =>
        i.logicalRelation.catalogTable.foreach { table =>
          addTableOrViewLevelObjs(
            table.identifier,
            outputObjs,
            currentDb,
            mode = overwriteToSaveMode(i.overwrite))
        }
        buildUnaryHivePrivObjs(
          i.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case i: InsertIntoDataSourceDirCommand =>
        buildUnaryHivePrivObjs(
          i.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case i: InsertIntoHadoopFsRelationCommand =>
        i.catalogTable foreach { t =>
          addTableOrViewLevelObjs(
            t.identifier,
            outputObjs,
            currentDb,
            i.partitionColumns.map(_.name).toList.asJava,
            t.schema.fieldNames.toList.asJava,
            i.mode)
        }
        buildUnaryHivePrivObjs(
          i.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case i: InsertIntoHiveDirCommand =>
        buildUnaryHivePrivObjs(
          i.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case i: InsertIntoHiveTable =>
        addTableOrViewLevelObjs(
          i.table.identifier, outputObjs, currentDb, mode = overwriteToSaveMode(i.overwrite))
        buildUnaryHivePrivObjs(
          i.query, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case l: LoadDataCommand =>
        addTableOrViewLevelObjs(
          l.table, outputObjs, currentDb, mode = overwriteToSaveMode(l.isOverwrite))

      case s: SaveIntoDataSourceCommand =>
        // TODO: mode missing
        buildUnaryHivePrivObjs(
          s.query, currentDb, outputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

      case s: SetDatabaseCommand => addDbLevelObjs(s.databaseName, inputObjs)

      case s: ShowColumnsCommand =>
        addTableOrViewLevelObjs(s.tableName, inputObjs, currentDb)

      case s: ShowCreateTableCommand => addTableOrViewLevelObjs(s.table, inputObjs, currentDb)

      case s: ShowFunctionsCommand => s.db.foreach(addDbLevelObjs(_, inputObjs))

      case s: ShowPartitionsCommand => addTableOrViewLevelObjs(s.tableName, inputObjs, currentDb)

      case s: ShowTablePropertiesCommand =>
        addTableOrViewLevelObjs(s.table, inputObjs, currentDb)

      case s: ShowTablesCommand => addDbLevelObjs(s.databaseName, inputObjs, currentDb)

      case s: TruncateTableCommand =>
        addTableOrViewLevelObjs(s.tableName, outputObjs, currentDb)



      case _ =>
      // AddFileCommand
      // AddJarCommand
      // AnalyzeColumnCommand
      // ClearCacheCommand
      // CreateTempViewUsing
      // ListFilesCommand
      // ListJarsCommand
      // RefreshTable
      // RefreshTable
      // ResetCommand
      // SetCommand
      // ShowDatabasesCommand
      // StreamingExplainCommand
      // UncacheTableCommand

    }
  }

  /**
   * Add database level hive privilege objects to input or output list
   * @param dbName database name as hive privilege object
   * @param hivePrivilegeObjects input or output list
   */
  private def addDbLevelObjs(
      dbName: String,
      hivePrivilegeObjects: JList[HivePrivilegeObject]): Unit = {
    hivePrivilegeObjects.add(
      HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add database level hive privilege objects to input or output list
   * @param dbOption an option of database name as hive privilege object
   * @param hivePrivilegeObjects input or output hive privilege object list
   */
  private def addDbLevelObjs(
      dbOption: Option[String],
      hivePrivilegeObjects: JList[HivePrivilegeObject],
      currentDb: String): Unit = {
    val dbName = dbOption.getOrElse(currentDb)
    hivePrivilegeObjects.add(
      HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add database level hive privilege objects to input or output list
   * @param tableIdentifier table identifier contains database name as hive privilege object
   * @param hivePrivilegeObjects input or output hive privilege object list
   */
  private def addDbLevelObjs(
      tableIdentifier: TableIdentifier,
      hivePrivilegeObjects: JList[HivePrivilegeObject],
      currentDb: String): Unit = {
    val dbName = tableIdentifier.database.getOrElse(currentDb)
    hivePrivilegeObjects.add(
      HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add table level hive privilege objects to input or output list
   * @param tableIdentifier table identifier contains database name, and table name as hive
   *                        privilege object
   * @param hivePrivilegeObjects input or output list
   * @param mode Append or overwrite
   */
  private def addTableOrViewLevelObjs(
      tableIdentifier: TableIdentifier,
      hivePrivilegeObjects: JList[HivePrivilegeObject],
      currentDb: String,
      partKeys: JList[String] = null,
      columns: JList[String] = null,
      mode: SaveMode = SaveMode.ErrorIfExists,
      cmdParams: JList[String] = null): Unit = {
    val dbName = tableIdentifier.database.getOrElse(currentDb)
    val tbName = tableIdentifier.table
    val hivePrivObjectActionType = getHivePrivObjActionType(mode)
    hivePrivilegeObjects.add(
      HivePrivilegeObjectHelper(
        HivePrivilegeObjectType.TABLE_OR_VIEW,
        dbName,
        tbName,
        partKeys,
        columns,
        hivePrivObjectActionType,
        cmdParams))
  }

  /**
   * Add function level hive privilege objects to input or output list
   * @param databaseName database name
   * @param functionName function name as hive privilege object
   * @param hivePrivilegeObjects input or output list
   */
  private def addFunctionLevelObjs(
      databaseName: Option[String],
      functionName: String,
      hivePrivilegeObjects: JList[HivePrivilegeObject],
      currentDb: String): Unit = {
    val dbName = databaseName.getOrElse(currentDb)
    hivePrivilegeObjects.add(
      HivePrivilegeObjectHelper(HivePrivilegeObjectType.FUNCTION, dbName, functionName))
  }

  /**
   * HivePrivObjectActionType INSERT or INSERT_OVERWRITE
   *
   * @param mode Append or Overwrite
   * @return
   */
  private def getHivePrivObjActionType(mode: SaveMode): HivePrivObjectActionType = {
    mode match {
      case SaveMode.Append => HivePrivObjectActionType.INSERT
      case SaveMode.Overwrite => HivePrivObjectActionType.INSERT_OVERWRITE
      case _ => HivePrivObjectActionType.OTHER
    }
  }

  /**
   * HivePrivObjectActionType INSERT or INSERT_OVERWRITE
   * @param overwrite Append or overwrite
   * @return
   */
  private def overwriteToSaveMode(overwrite: Boolean): SaveMode = {
    if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.ErrorIfExists
    }
  }
}
