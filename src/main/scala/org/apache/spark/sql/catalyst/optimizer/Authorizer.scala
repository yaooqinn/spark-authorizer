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
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAccessControlException, HiveAuthzContext, HiveOperationType}
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.{HivePrivObjsFromPlan, SessionStateOfHive}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

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
    SessionStateOfHive().foreach { state =>
      state.setIsHiveServerQuery(true)
      val hiveOperationType = toHiveOperationType(plan)
      val (in, out) = HivePrivObjsFromPlan(plan)
      val hiveAuthzContext = getHiveAuthzContext(plan, state)
      Option(state.getAuthorizerV2) match {
        case Some(authz) =>
          try {
            authz.checkPrivileges(hiveOperationType, in, out, hiveAuthzContext)
          } catch {
            case hae: HiveAccessControlException =>
              logError(
                s"""
                   |+===============================+
                   ||Spark SQL Authorization Failure|
                   ||-------------------------------|
                   ||${hae.getMessage}
                   ||-------------------------------|
                   ||Spark SQL Authorization Failure|
                   |+===============================+
                 """.stripMargin)
              throw hae
            case e: Exception => throw e
          }
        case None =>
          logWarning("Authorizer V2 not configured.")
      }
      // state
      plan match {
        case SetDatabaseCommand(databaseName) => state.setCurrentDatabase(databaseName)
        case _ =>
      }
    }
    // We just return the original plan here, so this rule will be executed only once
    plan
  }

  /**
    * Mapping of [[LogicalPlan]] -> [[HiveOperation]]
    * @param logicalPlan a spark LogicalPlan
    * @return
    */
  private def logicalPlan2HiveOperation(logicalPlan: LogicalPlan): HiveOperation = {
    logicalPlan match {
      case c: Command => c match {
        case ExplainCommand(child, _, _, _) => logicalPlan2HiveOperation(child)
        case StreamingExplainCommand(qe, _) => logicalPlan2HiveOperation(qe.optimizedPlan)
        case _: LoadDataCommand => HiveOperation.LOAD
        case _: InsertIntoHadoopFsRelationCommand => HiveOperation.QUERY
        case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
        case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
        case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
        case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
        case _: DropTableCommand => HiveOperation.DROPTABLE
        case _: DescribeTableCommand => HiveOperation.DESCTABLE
        case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
        case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
        case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
        case AlterTableRenameCommand(_, _, isView) =>
          if (!isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
        case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
        case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
        case _: AlterTableSetPropertiesCommand
             | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
        case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
        // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
        // Hive treat AnalyzeTableCommand as QUERY, obey it.
        case _: AnalyzeTableCommand => HiveOperation.QUERY
        case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
        case _: ShowTablesCommand => HiveOperation.SHOWTABLES
        case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
        case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
        case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
        case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
        case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
        case SetCommand(Some((_, None))) | SetCommand(None) => HiveOperation.SHOWCONF
        case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
        // Hive don't check privileges for `drop function command`, what about a unverified user
        // try to drop functions.
        // We treat permanent functions as tables for verifying.
        case DropFunctionCommand(_, _, _, false) => HiveOperation.DROPTABLE
        case DropFunctionCommand(_, _, _, true) => HiveOperation.DROPFUNCTION
        case _: CreateViewCommand
             | _: CacheTableCommand
             | _: CreateTempViewUsing => HiveOperation.CREATEVIEW
        case _: UncacheTableCommand => HiveOperation.DROPVIEW
        case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
        case _: CreateTable
             | _: CreateTableCommand
             | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
        case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
        case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
        case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
        case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
        // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS
        case _: AlterViewAsCommand => HiveOperation.QUERY
        case _ =>
          // AddFileCommand
          // AddJarCommand
          HiveOperation.EXPLAIN
      }
      case _: InsertIntoTable => HiveOperation.QUERY
      case _ => HiveOperation.QUERY
    }
  }

  private def toHiveOperationType(logicalPlan: LogicalPlan): HiveOperationType = {
    HiveOperationType.valueOf(logicalPlan2HiveOperation(logicalPlan).name())
  }

  /**
   * Provides context information in authorization check call that can be used for
   * auditing and/or authorization.
   */
  def getHiveAuthzContext(
      logicalPlan: LogicalPlan,
      state: SessionState,
      command: Option[String] = None): HiveAuthzContext = {
    val authzContextBuilder = new HiveAuthzContext.Builder()
    // set the ip address for user running the query, only for HiveServer2, Spark Applications
    // is more like the HiveServer2 itself, so I am just setting this for fun..
    authzContextBuilder.setUserIpAddress(state.getUserIpAddress)
    // set the sql query string, [[LogicalPlan]] contains such information in 2.2 or higher version
    // so this is for evolving..
    val cmd = command.getOrElse("")
    authzContextBuilder.setCommandString(cmd)
    authzContextBuilder.build()
  }
}