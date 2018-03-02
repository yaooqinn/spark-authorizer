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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.HivePrivilegeObjectHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

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

      case LogicalRelation(_, _, Some(table)) =>
        handleProjectionForRelation(table)

      case mr @ MetastoreRelation(_, _) =>
        handleProjectionForRelation(mr.catalogTable)

      case UnresolvedRelation(tableIdentifier, _) =>
        // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
        // Unfortunately, the real world is always a place where miracles happen.
        // We check the privileges directly without resolving the plan and leave everything
        // to spark to do.
        addTableOrViewLevelObjs(tableIdentifier, hivePrivilegeObjects, currentDb)

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

      case r: RunnableCommand => r match {
        case AlterDatabasePropertiesCommand(dbName, _) => addDbLevelObjs(dbName, outputObjs)

        case AlterTableAddPartitionCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableDropPartitionCommand(tableName, _, _, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableRecoverPartitionsCommand(tableName, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableRenameCommand(from, to, isView) if !isView || from.database.nonEmpty =>
          // rename tables / permanent views
          addTableOrViewLevelObjs(from, inputObjs, currentDb)
          addTableOrViewLevelObjs(to, outputObjs, currentDb)

        case AlterTableRenamePartitionCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableSerDePropertiesCommand(tableName, _, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableSetLocationCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableSetPropertiesCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterTableUnsetPropertiesCommand(tableName, _, _, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case AlterViewAsCommand(tableIdentifier, _, child) =>
          if (tableIdentifier.database.nonEmpty) {
            // it's a permanent view
            addTableOrViewLevelObjs(tableIdentifier, outputObjs, currentDb)
          }
          buildUnaryHivePrivObjs(child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case AnalyzeColumnCommand(tableIdent, columnNames) =>
          addTableOrViewLevelObjs(
            tableIdent, inputObjs, currentDb, columns = columnNames.toList.asJava)
          addTableOrViewLevelObjs(
            tableIdent, outputObjs, currentDb, columns = columnNames.toList.asJava)

        case AnalyzeTableCommand(tableName, _) =>
          val columns = new JAList[String]()
          columns.add("RAW__DATA__SIZE")
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb, columns = columns)
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case CacheTableCommand(_, plan, _) =>
          plan.foreach {
            buildUnaryHivePrivObjs(_, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
          }

        case CreateDatabaseCommand(databaseName, _, _, _, _) =>
          addDbLevelObjs(databaseName, outputObjs)

        case CreateDataSourceTableAsSelectCommand(table, mode, child) =>
          addDbLevelObjs(table.identifier, outputObjs, currentDb)
          addTableOrViewLevelObjs(table.identifier, outputObjs, currentDb, mode = mode)
          buildBinaryHivePrivObject(child, currentDb, inputObjs, outputObjs)

        case CreateDataSourceTableCommand(table, _) =>
          addTableOrViewLevelObjs(table.identifier, outputObjs, currentDb)

        case CreateFunctionCommand(databaseName, functionName, _, _, false) =>
          addDbLevelObjs(databaseName, outputObjs, currentDb)
          addFunctionLevelObjs(databaseName, functionName, outputObjs, currentDb)

        case CreateHiveTableAsSelectCommand(tableDesc, child, _) =>
          addDbLevelObjs(tableDesc.identifier, outputObjs, currentDb)
          addTableOrViewLevelObjs(tableDesc.identifier, outputObjs, currentDb)
          buildBinaryHivePrivObject(child, currentDb, inputObjs, outputObjs)

        case CreateTableCommand(table, _) =>
          addTableOrViewLevelObjs(table.identifier, outputObjs, currentDb)

        case CreateTableLikeCommand(targetTable, sourceTable, _) =>
          addDbLevelObjs(targetTable, outputObjs, currentDb)
          addTableOrViewLevelObjs(targetTable, outputObjs, currentDb)
          // hive don't handle source table's privileges, we should not obey that, because
          // it will cause meta information leak
          addDbLevelObjs(sourceTable, inputObjs, currentDb)
          addTableOrViewLevelObjs(sourceTable, inputObjs, currentDb)

        case CreateViewCommand(viewName, _, _, _, _, child, _, _, viewType) =>
          viewType match {
            case PersistedView =>
              // PersistedView will be tied to a database
              addDbLevelObjs(viewName, outputObjs, currentDb)
              addTableOrViewLevelObjs(viewName, outputObjs, currentDb)
            case _ =>
          }
          buildUnaryHivePrivObjs(child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case DescribeDatabaseCommand(databaseName, _) =>
          addDbLevelObjs(databaseName, inputObjs)

        case DescribeFunctionCommand(functionName, _) =>
          addFunctionLevelObjs(functionName.database, functionName.funcName, inputObjs, currentDb)

        case DescribeTableCommand(table, _, _, _) =>
          addTableOrViewLevelObjs(table, inputObjs, currentDb)

        case DropDatabaseCommand(databaseName, _, _) =>
          // outputObjs are enough for privilege check, adding inputObjs for consistency with hive
          // behaviour in case of some unexpected issues.
          addDbLevelObjs(databaseName, inputObjs)
          addDbLevelObjs(databaseName, outputObjs)

        case DropFunctionCommand(databaseName, functionName, _, _) =>
          addFunctionLevelObjs(databaseName, functionName, outputObjs, currentDb)

        case DropTableCommand(tableName, _, false, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case ExplainCommand(child, _, _) =>
          buildBinaryHivePrivObject(child, currentDb, inputObjs, outputObjs)

        case InsertIntoDataSourceCommand(logicalRelation, child, overwrite) =>
          logicalRelation.catalogTable.foreach { table =>
            addTableOrViewLevelObjs(
              table.identifier,
              outputObjs, currentDb, mode = overwriteToSaveMode(overwrite.enabled))
          }
          buildUnaryHivePrivObjs(child, currentDb, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

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

        case LoadDataCommand(table, _, _, isOverwrite, _) =>
          addTableOrViewLevelObjs(
            table, outputObjs, currentDb, mode = overwriteToSaveMode(isOverwrite))

        case SetDatabaseCommand(databaseName) => addDbLevelObjs(databaseName, inputObjs)

        case ShowColumnsCommand(_, tableName) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)

        case ShowCreateTableCommand(table) => addTableOrViewLevelObjs(table, inputObjs, currentDb)

        case ShowFunctionsCommand(db, _, _, _) => db.foreach(addDbLevelObjs(_, inputObjs))

        case ShowPartitionsCommand(tableName, _) =>
          addTableOrViewLevelObjs(tableName, inputObjs, currentDb)

        case ShowTablePropertiesCommand(table, _) =>
          addTableOrViewLevelObjs(table, inputObjs, currentDb)

        case ShowTablesCommand(db, _) => addDbLevelObjs(db, inputObjs, currentDb)

        case TruncateTableCommand(tableName, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs, currentDb)

        case _ =>
        // AddFileCommand
        // AddJarCommand
        // AnalyzeColumnCommand
        // CreateTempViewUsing
        // ListFilesCommand
        // ListJarsCommand
        // RefreshTable
        // RefreshTable
        // ResetCommand
        // SetCommand
        // ShowDatabasesCommand
        // UncacheTableCommand
      }

      case _ =>
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
