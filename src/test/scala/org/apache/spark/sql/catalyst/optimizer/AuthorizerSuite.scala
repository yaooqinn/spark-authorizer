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

import org.apache.hadoop.hive.ql.plan.HiveOperation

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AlterDatabasePropertiesCommand, AlterTableDropPartitionCommand}

/**
 * TODO: Add UTs
 */
class AuthorizerSuite extends SparkFunSuite {

  test("spark logical plan to hive operation") {
    val db = "test_database"
    val tbl1 = "test_table_1"
    val emptyStringMap = Map.empty[String, String]
    val atpc = AlterDatabasePropertiesCommand(db, emptyStringMap)
    val atpcHiveType = Authorizer.getHiveOperation(atpc)
    assert(atpcHiveType === HiveOperation.ALTERDATABASE)
    val identifier = TableIdentifier(tbl1, Some(db))
    val alterTableDropPartitionCommand = AlterTableDropPartitionCommand(
      identifier, Seq(emptyStringMap), ifExists = true, purge = true, retainData = true)
    val ataccType = Authorizer.getHiveOperation(alterTableDropPartitionCommand)
    assert(ataccType === HiveOperation.ALTERTABLE_DROPPARTS)
  }

}
