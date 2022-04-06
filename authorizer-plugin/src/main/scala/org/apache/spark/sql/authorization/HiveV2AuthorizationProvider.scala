/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.spark.sql.authorization

import scala.util.Try

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.client.AuthzImpl


class HiveV2AuthorizationProvider extends AuthorizationProvider{
  override def checkPrivileges(
    spark: SparkSession,
    requests: Seq[AuthorizationRequest]
  ): Seq[Boolean] = {
    val authzContext = new HiveAuthzContext.Builder().build()
    requests.map{req =>
      Try(
        AuthzImpl.checkPrivileges(
          spark,
          req.hiveOpType,
          req.inputObjs,
          req.outputObjs,
          authzContext)
      ).isSuccess
    }
  }

  override def checkPrivileges(spark: SparkSession, req: AuthorizationRequest): Unit = {
    val authzContext = new HiveAuthzContext.Builder().build()
    AuthzImpl.checkPrivileges(
      spark,
      req.hiveOpType,
      req.inputObjs,
      req.outputObjs,
      authzContext)
  }
}
