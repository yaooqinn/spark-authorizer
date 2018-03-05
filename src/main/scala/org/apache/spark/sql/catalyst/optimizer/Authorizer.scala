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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAuthzContext, HiveOperationType}

import org.apache.spark.sql.{Authorizable, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{DefaultAuthorizerImpl, HiveExternalCatalog, HivePrivObjsFromPlan}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

/**
 * Do Hive Authorizing V2, with `Apache Ranger` ranger-hive-plugin well configured,
 * This [[Rule]] provides you with column level fine-gained SQL Standard Authorization.
 * Usage:
 *   1. cp spark-authorizer-<version>.jar $SPARK_HOME/jars
 *   2. install ranger-hive-plugin for spark
 *   3. configure you hive-site.xml and ranger configuration file as shown in [./conf]
 *   4. import org.apache.spark.sql.catalyst.optimizer.Authorizer
 *   5. spark.experimental.extraOptimizations ++= Seq(Authorizer)
 *   6. then suffer for the authorizing pain
 */
object Authorizer extends Rule[LogicalPlan] {

  /**
   * Visit the [[LogicalPlan]] recursively to get all hive privilege objects, check the privileges
   * using Hive Authorizer V2 which provide sql based authorization and can implements
   * ranger-hive-plugins.
   * If the user is authorized, then the original plan will be returned; otherwise, interrupted by
   * some particular privilege exceptions.
   * @param plan a spark LogicalPlan for verifying privileges
   * @return a plan itself which has gone through the privilege check.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val hiveOperationType = toHiveOperationType(plan)
    val hiveAuthzContext = getHiveAuthzContext(plan)
    SparkSession.getActiveSession match {
      case Some(session) =>
        session.sharedState.externalCatalog match {
          case catalog: HiveExternalCatalog =>
            catalog.client match {
              case authz: Authorizable =>
                val (in, out) = HivePrivObjsFromPlan.build(plan, authz.currentDatabase())
                authz.checkPrivileges(hiveOperationType, in, out, hiveAuthzContext)
              case _ =>
                val (in, out) = HivePrivObjsFromPlan.build(plan, defaultAuthz.currentDatabase())
                defaultAuthz.checkPrivileges(hiveOperationType, in, out, hiveAuthzContext)
            }
          case _ =>
        }
      case None =>
    }

    // We just return the original plan here, so this rule will be executed only once
    plan
  }

  private lazy val defaultAuthz = new DefaultAuthorizerImpl

  /**
    * Mapping of [[LogicalPlan]] -> [[HiveOperation]]
    * @param logicalPlan a spark LogicalPlan
    * @return
    */
  private def logicalPlan2HiveOperation(logicalPlan: LogicalPlan): HiveOperation = {
    logicalPlan match {
      case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
      case _: AlterTableAddColumnsCommand => HiveOperation.ALTERTABLE_ADDCOLS
      case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
      case _: AlterTableChangeColumnCommand => HiveOperation.ALTERTABLE_RENAMECOL
      case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
      case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
      case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
      case a: AlterTableRenameCommand =>
        if (!a.isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
      case _: AlterTableSetPropertiesCommand
           | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
      case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
      case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
      case _: AlterViewAsCommand => HiveOperation.QUERY
      // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS

      case _: AnalyzeColumnCommand => HiveOperation.QUERY
      // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
      // Hive treat AnalyzeTableCommand as QUERY, obey it.
      case _: AnalyzeTableCommand => HiveOperation.QUERY
      case _: AnalyzePartitionCommand => HiveOperation.QUERY


      case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
      case _: CreateDataSourceTableAsSelectCommand
           | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
      case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
      case _: CreateTable
           | _: CreateTableCommand
           | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
      case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
      case _: CreateViewCommand
           | _: CacheTableCommand
           | _: CreateTempViewUsing => HiveOperation.CREATEVIEW

      case _: DescribeColumnCommand => HiveOperation.DESCTABLE
      case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
      case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
      case _: DescribeTableCommand => HiveOperation.DESCTABLE

      case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
      // Hive don't check privileges for `drop function command`, what about a unverified user
      // try to drop functions.
      // We treat permanent functions as tables for verifying.
      case d: DropFunctionCommand if !d.isTemp => HiveOperation.DROPTABLE
      case d: DropFunctionCommand if d.isTemp => HiveOperation.DROPFUNCTION
      case _: DropTableCommand => HiveOperation.DROPTABLE

      case e: ExplainCommand => logicalPlan2HiveOperation(e.logicalPlan)

      case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
      case _: InsertIntoDataSourceDirCommand => HiveOperation.QUERY
      case _: InsertIntoHadoopFsRelationCommand => HiveOperation.CREATETABLE_AS_SELECT
      case _: InsertIntoHiveDirCommand => HiveOperation.QUERY
      case _: InsertIntoHiveTable => HiveOperation.QUERY
      case _: InsertIntoTable => HiveOperation.CREATETABLE_AS_SELECT

      case _: LoadDataCommand => HiveOperation.LOAD

      case _: SaveIntoDataSourceCommand => HiveOperation.QUERY
      case s: SetCommand if s.kv.isEmpty || s.kv.get._2.isEmpty => HiveOperation.SHOWCONF
      case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
      case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
      case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
      case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
      case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
      case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
      case _: ShowTablesCommand => HiveOperation.SHOWTABLES
      case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
      case s: StreamingExplainCommand =>
        logicalPlan2HiveOperation(s.queryExecution.optimizedPlan)

      case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE

      case _: UncacheTableCommand => HiveOperation.DROPVIEW

      // Commands that do not need build privilege goes as explain type
      case _: Command =>
        // AddFileCommand
        // AddJarCommand
        // ...
        HiveOperation.EXPLAIN

      case _ => HiveOperation.QUERY
    }
  }

  private[this] def toHiveOperationType(logicalPlan: LogicalPlan): HiveOperationType = {
    HiveOperationType.valueOf(logicalPlan2HiveOperation(logicalPlan).name())
  }

  /**
   * Provides context information in authorization check call that can be used for
   * auditing and/or authorization.
   */
  private[this] def getHiveAuthzContext(
      logicalPlan: LogicalPlan,
      command: Option[String] = None): HiveAuthzContext = {
    val authzContextBuilder = new HiveAuthzContext.Builder()
    // set the sql query string, [[LogicalPlan]] contains such information in 2.2 or higher version
    // so this is for evolving..
    val cmd = command.getOrElse("")
    authzContextBuilder.setCommandString(cmd)
    authzContextBuilder.build()
  }
}