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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.optimizer.Authorizer
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

/**
 * TODO: Add UTs
 */
class AuthorizerSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._

  private val user = Utils.getCurrentUserName()

  var jsonFilePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }

  {
    Seq("hive.security.authorization.enabled" -> "true",
      "hive.security.authorization.createtable.owner.grants" -> "ALL").foreach { case (k, v) =>
      spark.sparkContext.hadoopConfiguration.set(k, v)
    }

    spark.experimental.extraOptimizations ++= Seq(Authorizer)

    hiveClient.runSqlHive("create role testrole")
  }

  private def hiveClient = sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  test("Hive security authorization enabled") {
    assert(hiveClient.getConf("hive.security.authorization.enabled", "false") === "true")
    assert(hiveClient.getConf("hive.security.authorization.createtable.owner.grants", "") === "ALL")

  }
}
