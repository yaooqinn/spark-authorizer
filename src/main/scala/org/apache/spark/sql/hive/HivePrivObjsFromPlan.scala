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
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.HivePrivilegeObjectHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

/**
 * [[LogicalPlan]] -> list of [[HivePrivilegeObject]]s
 */
private[sql] object HivePrivObjsFromPlan {

  def apply(logicalPlan: LogicalPlan): (JList[HivePrivilegeObject], JList[HivePrivilegeObject]) = {
    val inputObjs = new JAList[HivePrivilegeObject]
    val outputObjs = new JAList[HivePrivilegeObject]
    logicalPlan match {
      case cmd: Command => buildInOutHivePrivObject(cmd, inputObjs, outputObjs)
      case iit: InsertIntoTable => buildInOutHivePrivObject(iit, inputObjs, outputObjs)
      case _ => buildInputHivePrivObjs(logicalPlan, inputObjs)
    }
    (inputObjs, outputObjs)
  }

  private def buildInputHivePrivObjs(
      logicalPlan: LogicalPlan,
      inputObjs: JList[HivePrivilegeObject],
      hivePrivObjType: HivePrivilegeObjectType = HivePrivilegeObjectType.TABLE_OR_VIEW,
      projectionList: Seq[NamedExpression] = Seq.empty): Unit = {
    logicalPlan match {
      case Project(projList, child) =>
        buildInputHivePrivObjs(
          child,
          inputObjs,
          HivePrivilegeObjectType.TABLE_OR_VIEW,
          projList)

      case LogicalRelation(_, _, Some(table)) =>
        val partKeys = if (projectionList.isEmpty) {
          table.partitionColumnNames
        } else {
          table.partitionColumnNames.filter(projectionList.map(_.name).contains(_))
        }
        val fieldNames = if (projectionList.isEmpty) {
          table.schema.fieldNames
        } else {
          table.schema.fieldNames.filter(projectionList.map(_.name).contains(_))
        }

        addTableOrViewLevelObjs(
          table.identifier,
          inputObjs,
          partKeys.toList.asJava,
          fieldNames.toList.asJava)

      case mr @ MetastoreRelation(_, _) =>
        val partKeys = if (projectionList.isEmpty) {
          mr.catalogTable.partitionColumnNames
        } else {
          mr.catalogTable.partitionColumnNames.filter(projectionList.map(_.prettyName).contains(_))
        }
        val fieldNames = if (projectionList.isEmpty) {
          mr.catalogTable.schema.fieldNames
        } else {
          mr.catalogTable.schema.fieldNames.filter(projectionList.map(_.prettyName).contains(_))
        }

        addTableOrViewLevelObjs(
          mr.catalogTable.identifier,
          inputObjs,
          partKeys.toList.asJava,
          fieldNames.toList.asJava)

      case UnresolvedRelation(tableIdentifier, _) =>
        // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
        // Unfortunately, the real world is always a place where miracles happen.
        // We check the privileges directly without resolving the plan and leave everything
        // to spark to do.
        addTableOrViewLevelObjs(tableIdentifier, inputObjs)

      case bn: BinaryNode =>
        buildInputHivePrivObjs(bn.left, inputObjs, hivePrivObjType, projectionList)
        buildInputHivePrivObjs(bn.right, inputObjs, hivePrivObjType, projectionList)

      case un: UnaryNode =>
        buildInputHivePrivObjs(un.child, inputObjs, hivePrivObjType, projectionList)

      case Union(children) =>
        for (child <- children) {
          buildInputHivePrivObjs(child, inputObjs, hivePrivObjType, projectionList)
        }

      case _ =>
    }
  }

  private def buildInOutHivePrivObject(
      logicalPlan: LogicalPlan,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject]): Unit = {
    logicalPlan match {
      case CreateTable(tableDesc, mode, maybePlan) =>
        addDbLevelObjs(tableDesc.identifier, outputObjs)
        addTableOrViewLevelObjs(tableDesc.identifier, outputObjs, mode = mode)
        maybePlan.foreach {
          buildInputHivePrivObjs(_, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)
        }

      case r: RunnableCommand => r match {
        case AlterDatabasePropertiesCommand(dbName, _) => addDbLevelObjs(dbName, outputObjs)

        case AlterTableAddPartitionCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableDropPartitionCommand(tableName, _, _, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableRecoverPartitionsCommand(tableName, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableRenameCommand(from, to, isView) if !isView || from.database.nonEmpty =>
          // rename tables / permanent views
          addTableOrViewLevelObjs(from, inputObjs)
          addTableOrViewLevelObjs(to, outputObjs)

        case AlterTableRenamePartitionCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableSerDePropertiesCommand(tableName, _, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableSetLocationCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableSetPropertiesCommand(tableName, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterTableUnsetPropertiesCommand(tableName, _, _, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case AlterViewAsCommand(tableIdentifier, _, child) =>
          if (tableIdentifier.database.nonEmpty) {
            // it's a permanent view
            addTableOrViewLevelObjs(tableIdentifier, outputObjs)
          }
          buildInputHivePrivObjs(child, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case AnalyzeTableCommand(tableName, _) =>
          val columns = new JAList[String]()
          columns.add("RAW__DATA__SIZE")
          addTableOrViewLevelObjs(tableName, inputObjs, columns = columns)
          addTableOrViewLevelObjs(tableName, outputObjs)

        case CacheTableCommand(_, plan, _) =>
          plan.foreach {buildInputHivePrivObjs(_, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)}

        case CreateDatabaseCommand(databaseName, _, _, _, _) =>
          addDbLevelObjs(databaseName, outputObjs)

        case CreateDataSourceTableAsSelectCommand(table, mode, child) =>
          addDbLevelObjs(table.identifier, outputObjs)
          addTableOrViewLevelObjs(table.identifier, outputObjs, mode = mode)
          buildInputHivePrivObjs(child, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case CreateDataSourceTableCommand(table, _) =>
          addTableOrViewLevelObjs(table.identifier, outputObjs)

        case CreateFunctionCommand(databaseName, functionName, _, _, false) =>
          addDbLevelObjs(databaseName, outputObjs)
          addFunctionLevelObjs(databaseName, functionName, outputObjs)

        case CreateHiveTableAsSelectCommand(tableDesc, child, _) =>
          addDbLevelObjs(tableDesc.identifier, outputObjs)
          addTableOrViewLevelObjs(tableDesc.identifier, outputObjs)
          buildInputHivePrivObjs(child, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case CreateTableCommand(table, _) =>
          addTableOrViewLevelObjs(table.identifier, outputObjs)

        case CreateTableLikeCommand(targetTable, sourceTable, _) =>
          addDbLevelObjs(targetTable, outputObjs)
          addTableOrViewLevelObjs(targetTable, outputObjs)
          // hive don't handle source table's privileges, we should not obey that, because
          // it will cause meta information leak
          addDbLevelObjs(sourceTable, inputObjs)
          addTableOrViewLevelObjs(sourceTable, inputObjs)

        case CreateViewCommand(viewName, _, _, _, _, child, _, _, viewType) =>
          viewType match {
            case PersistedView =>
              // PersistedView will be tied to a database
              addDbLevelObjs(viewName, outputObjs)
              addTableOrViewLevelObjs(viewName, outputObjs)
            case _ =>
          }
          buildInputHivePrivObjs(child, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case DescribeDatabaseCommand(databaseName, _) =>
          addDbLevelObjs(databaseName, inputObjs)

        case DescribeFunctionCommand(functionName, _) =>
          addFunctionLevelObjs(functionName.database, functionName.funcName, inputObjs)

        case DescribeTableCommand(table, _, _, _) => addTableOrViewLevelObjs(table, inputObjs)

        case DropDatabaseCommand(databaseName, _, _) =>
          // outputObjs are enough for privilege check, adding inputObjs for consistency with hive
          // behaviour in case of some unexpected issues.
          addDbLevelObjs(databaseName, inputObjs)
          addDbLevelObjs(databaseName, outputObjs)

        case DropFunctionCommand(databaseName, functionName, _, _) =>
          addFunctionLevelObjs(databaseName, functionName, outputObjs)

        case DropTableCommand(tableName, _, false, _) =>
          addTableOrViewLevelObjs(tableName, outputObjs)

        case ExplainCommand(child, _, _) =>
          buildInOutHivePrivObject(child, inputObjs, outputObjs)

        case InsertIntoDataSourceCommand(logicalRelation, child, overwrite) =>
          logicalRelation.catalogTable.foreach { table =>
            addTableOrViewLevelObjs(
              table.identifier, outputObjs, mode = overwriteToSaveMode(overwrite.enabled))
          }
          buildInputHivePrivObjs(child, inputObjs, HivePrivilegeObjectType.TABLE_OR_VIEW)

        case LoadDataCommand(table, _, _, isOverwrite, _) =>
          addTableOrViewLevelObjs(table, outputObjs, mode = overwriteToSaveMode(isOverwrite))

        case SetDatabaseCommand(databaseName) => addDbLevelObjs(databaseName, inputObjs)

        case ShowColumnsCommand(_, tableName) => addTableOrViewLevelObjs(tableName, inputObjs)

        case ShowCreateTableCommand(table) => addTableOrViewLevelObjs(table, inputObjs)

        case ShowFunctionsCommand(db, _, _, _) => db.foreach(addDbLevelObjs(_, inputObjs))

        case ShowPartitionsCommand(tableName, _) => addTableOrViewLevelObjs(tableName, inputObjs)

        case ShowTablePropertiesCommand(table, _) => addTableOrViewLevelObjs(table, inputObjs)

        case ShowTablesCommand(db, _) => addDbLevelObjs(db, inputObjs)

        case TruncateTableCommand(tableName, _) => addTableOrViewLevelObjs(tableName, inputObjs)

        case _ =>
        // AddFileCommand
        // AddJarCommand
        // AnalyzeColumnCommand
        // CreateTempViewUsing
        // InsertIntoHadoopFsRelationCommand
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
   * @param dbName
   * @param objs input or output list
   */
  private def addDbLevelObjs(dbName: String, objs: JList[HivePrivilegeObject]): Unit = {
    objs.add(HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add database level hive privilege objects to input or output list
   * @param dbOption
   * @param objs
   */
  private def addDbLevelObjs(
      dbOption: Option[String],
      objs: JList[HivePrivilegeObject]): Unit = {
    val dbName = dbOption.getOrElse(this.getCurrentDatabase())
    objs.add(HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add database level hive privilege objects to input or output list
   * @param tableIdentifier
   * @param objs
   */
  private def addDbLevelObjs(
      tableIdentifier: TableIdentifier,
      objs: JList[HivePrivilegeObject]): Unit = {
    val dbName = tableIdentifier.database.getOrElse(this.getCurrentDatabase())
    objs.add(HivePrivilegeObjectHelper(HivePrivilegeObjectType.DATABASE, dbName, dbName))
  }

  /**
   * Add table level hive privilege objects to input or output list
   * @param tableName
   * @param objs input or output list
   * @param mode
   */
  private def addTableOrViewLevelObjs(
      tableName: TableIdentifier,
      objs: JList[HivePrivilegeObject],
      partKeys: JList[String] = null,
      columns: JList[String] = null,
      mode: SaveMode = SaveMode.ErrorIfExists,
      cmdParams: JList[String] = null): Unit = {
    val dbName = tableName.database.getOrElse(this.getCurrentDatabase())
    val tbName = tableName.table
    val hivePrivObjectActionType = getHivePrivObjActionType(mode)
    objs.add(
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
   * @param databaseName
   * @param functionName
   * @param objs input or output list
   */
  private def addFunctionLevelObjs(
      databaseName: Option[String],
      functionName: String,
      objs: JList[HivePrivilegeObject]): Unit = {
    val dbName = databaseName.getOrElse(this.getCurrentDatabase())
    objs.add(HivePrivilegeObjectHelper(HivePrivilegeObjectType.FUNCTION, dbName, functionName))
  }

  /**
    * HivePrivObjectActionType INSERT or INSERT_OVERWRITE
    *
    * @param mode
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
   * @param overwrite
   * @return
   */
  private def overwriteToSaveMode(overwrite: Boolean): SaveMode = {
    if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.ErrorIfExists
    }
  }

  /**
   * Get the current database
   * @return
   */
  private def getCurrentDatabase(): String = {
    SessionStateOfHive().get.getCurrentDatabase
  }
}
