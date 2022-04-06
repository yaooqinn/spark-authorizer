/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import java.io.File

import com.githup.yaooqinn.spark.authorizer.Logging
import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAccessControlException, HiveOperationType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.authorization.{AuthorizationProvider, AuthorizationRequest}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.{HiveExternalCatalog, PrivilegesBuilder}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.util.Utils

trait Authorizable extends Rule[LogicalPlan] with Logging {

  def spark: SparkSession

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
    val operationType: HiveOperationType = getOperationType(plan)
    val (in, out) = PrivilegesBuilder.build(plan)
    spark.sharedState.externalCatalog match {
      case _: HiveExternalCatalog =>
        try {
          AuthorizationProvider.checkPrivileges(
            spark,
            new AuthorizationRequest(operationType, in, out)
          )
        } catch {
          case hae: HiveAccessControlException =>
            error(
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
      case _ =>
    }
    // iff no exception.
    // We just return the original plan here, so this rule will be executed only once.
    plan
  }

  def policyCacheDir: Option[String] = {
    Option(spark.sparkContext.hadoopConfiguration.get("ranger.plugin.hive.policy.cache.dir"))
  }


  def createCacheDirIfNonExists(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      if (file.mkdirs()) {
        info("Creating ranger policy cache directory at " + file.getAbsolutePath)
        file.deleteOnExit()
      } else {
        warn("Unable to create ranger policy cache directory at " + file.getAbsolutePath)
      }
    }
  }

  policyCacheDir match {
    case Some(dir) => createCacheDirIfNonExists(dir)
    case _ =>
      // load resources from ranger configuration files
      Option(Utils.getContextOrSparkClassLoader.getResource("ranger-hive-security.xml")) match {
        case Some(url) =>
          spark.sparkContext.hadoopConfiguration.addResource(url)
          policyCacheDir match {
            case Some(dir) => createCacheDirIfNonExists(dir)
            case _ =>
          }
        case _ =>
      }
  }

  /**
   * Mapping of [[LogicalPlan]] -> [[HiveOperation]]
   * @param plan a spark LogicalPlan
   * @return
   */
  def getHiveOperation(plan: LogicalPlan): HiveOperation = {
    plan match {
      case c: Command => c match {
        case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
        case p if p.nodeName == "AlterTableAddColumnsCommand" => HiveOperation.ALTERTABLE_ADDCOLS
        case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
        case p if p.nodeName == "AlterTableChangeColumnCommand" =>
          HiveOperation.ALTERTABLE_RENAMECOL
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
        case p if p.nodeName == "AnalyzePartitionCommand" => HiveOperation.QUERY

        case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
        case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
        case _: CreateTableCommand
             | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
        case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
        case _: CreateViewCommand
             | _: CacheTableCommand
             | _: CreateTempViewUsing => HiveOperation.CREATEVIEW

        case p if p.nodeName == "DescribeColumnCommand" => HiveOperation.DESCTABLE
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

        case e: ExplainCommand => getHiveOperation(e.logicalPlan)

        case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
        case p if p.nodeName == "InsertIntoDataSourceDirCommand" => HiveOperation.QUERY
        case _: InsertIntoHadoopFsRelationCommand => HiveOperation.CREATETABLE_AS_SELECT
        case p if p.nodeName == "InsertIntoHiveDirCommand" => HiveOperation.QUERY
        case p if p.nodeName == "InsertIntoHiveTable" => HiveOperation.QUERY

        case _: LoadDataCommand => HiveOperation.LOAD

        case p if p.nodeName == "SaveIntoDataSourceCommand" => HiveOperation.QUERY
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
          getHiveOperation(s.queryExecution.optimizedPlan)

        case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE

        case _: UncacheTableCommand => HiveOperation.DROPVIEW

        // Commands that do not need build privilege goes as explain type
        case _ =>
          // AddFileCommand
          // AddJarCommand
          // ...
          HiveOperation.EXPLAIN
      }
      case _ => HiveOperation.QUERY
    }
  }

  def getOperationType(logicalPlan: LogicalPlan): HiveOperationType = {
    HiveOperationType.valueOf(getHiveOperation(logicalPlan).name())
  }
}
