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

package org.apache.spark.sql.execution.command

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hive.SessionStateCacheManager


/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
 *   SHOW TABLE EXTENDED [(IN|FROM) database_name] LIKE 'identifier_with_wildcards'
 *   [PARTITION(partition_spec)];
 * }}}
 *
 * NOTES: An authorized replacement for the original [[ShowTablesCommand]]
 */
case class AuthorizedShowTablesCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    val state = SessionStateCacheManager.get().getState
    val tables = if (state != null) {
      val client = Hive.get(state.getConf)
      SessionState.setCurrentSessionState(state)
      tableIdentifierPattern.map(client.getTablesByPattern(db, _))
        .getOrElse(client.getAllTables(db)).asScala.map(TableIdentifier(_, Some(db)))
    } else {
      tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
    }

    tables.map { tableIdent =>
      val isTemp = catalog.isTemporaryTable(tableIdent)
      Row(tableIdent.database.getOrElse(""), tableIdent.table, isTemp)
    }
  }
}
