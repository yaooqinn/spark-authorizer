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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.SessionStateCacheManager

case class AuthorizedShowTablesCommand(
    override val databaseName: Option[String],
    override val tableIdentifierPattern: Option[String])
  extends ShowTablesCommand(databaseName, tableIdentifierPattern) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    val state = SessionStateCacheManager.get().getState()
    val tables = if (state != null) {
      val client = Hive.get(state.getConf)
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
