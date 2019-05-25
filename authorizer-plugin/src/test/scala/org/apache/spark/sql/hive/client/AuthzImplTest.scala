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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils.isCliSessionState
import org.apache.spark.sql.hive.{AuthzUtils, HiveExternalCatalog}


class AuthzImplTest extends SparkFunSuite {

    test("Read extra configuration file") {
        val extraConfig = AuthzImpl.extraConfig
        assert(extraConfig("hive.security.authorization.enabled") === "true")
    }

    test("CachedHiveClient should be separated builtin hive client") {
        val spark: SparkSession = SparkSession
            .builder()
            .config(
                "spark.sql.extensions",
                "org.apache.ranger.authorization.spark.authorizer.RangerSparkSQLExtension"
            )
            .master("local[1]")
            .appName("SparkTest")
            .enableHiveSupport()
            .getOrCreate()
        val client = spark.sharedState
            .externalCatalog.asInstanceOf[HiveExternalCatalog]
            .client
        val hiveClient = AuthzImpl.getCachedHiveClient(spark)
        assert(hiveClient.version.fullVersion === "1.2.2")
        assert(hiveClient != null)
        assert(hiveClient != client)
    }

    test("Get Authorizer V2") {
        val spark: SparkSession = SparkSession
            .builder()
            .config(
                "spark.sql.extensions",
                "org.apache.ranger.authorization.spark.authorizer.RangerSparkSQLExtension"
            )
            .master("local[1]")
            .appName("SparkTest")
            .enableHiveSupport()
            .getOrCreate()

        val client = spark.sharedState
            .externalCatalog.asInstanceOf[HiveExternalCatalog]
            .client.asInstanceOf[HiveClient]

        assert(client.getConf("hive.security.authorization.enabled", "false") == "false")

        spark.sql("show tables").show(false)

        val clientImpl: HiveClientImpl = AuthzUtils.getFieldVal(AuthzImpl, "_cachedHiveClient")
            .asInstanceOf[HiveClientImpl]
        val authz = clientImpl.state.getAuthorizerV2
        assert(authz != null)
    }


}
