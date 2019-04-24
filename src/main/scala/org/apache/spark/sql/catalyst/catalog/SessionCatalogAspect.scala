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


package org.apache.spark.sql.catalyst.catalog

import java.util.{ArrayList => JAList}

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAuthzContext, HiveOperationType, HivePrivilegeObject => HPO}
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{Around, Aspect}
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.optimizer.HivePrivilegeObject
import org.apache.spark.sql.hive.PrivilegesBuilder
import org.apache.spark.sql.hive.client.AuthzImpl


/**
 * Aspect for filtering SessionCatalog listDatabase and listTables result,
 * only return the resources that user have privileges to access to.
 */
@Aspect
class SessionCatalogAspect extends Logging {

  private lazy val spark: SparkSession = SparkSession.getActiveSession
    .getOrElse(SparkSession.getDefaultSession.get)

  logInfo("SessionatalogAspect initiating")

  @Around(
    "execution(public * org.apache.spark.sql.catalyst.catalog.SessionCatalog.listDatabases())"
  )
  def filterListDatabasesResult(pjp: ProceedingJoinPoint): Seq[String] = {
    logDebug("filterListDatabasesResult")
    val dbs = pjp.proceed.asInstanceOf[Seq[String]]
    val operationType: HiveOperationType = HiveOperationType.SWITCHDATABASE
    val authzContext = new HiveAuthzContext.Builder().build()
    dbs.filter { db =>
      val inputObjs = new JAList[HPO]
      val outputObjs = new JAList[HPO]
      inputObjs.add(
        HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, db, db))
      Try(AuthzImpl.checkPrivileges(
        spark, operationType, inputObjs, outputObjs, authzContext, false
      )).isSuccess
    }
  }

  @Around(
    "execution(public * org.apache.spark.sql.catalyst.catalog.SessionCatalog.listTables(" +
      "java.lang.String, java.lang.String" +
      "))"
  )
  def filterListTablesResult(pjp: ProceedingJoinPoint): Seq[TableIdentifier] = {
    logDebug("filterListTablesResult")
    val tables = pjp.proceed.asInstanceOf[Seq[TableIdentifier]]
    val operationType: HiveOperationType = HiveOperationType.SHOW_TABLESTATUS
    val authzContext = new HiveAuthzContext.Builder().build()
    tables.filter { table =>
      val inputObjs = new JAList[HPO]
      val outputObjs = new JAList[HPO]
      val tableMeta = spark.sessionState.catalog.getTableMetadata(table)
      PrivilegesBuilder.addTableOrViewLevelObjs(
        tableMeta.identifier,
        inputObjs,
        tableMeta.partitionColumnNames,
        tableMeta.schema.fieldNames)
      Try(AuthzImpl.checkPrivileges(
        spark, operationType, inputObjs, outputObjs, authzContext, false
      )).isSuccess
    }
  }

}
