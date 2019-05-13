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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

abstract class AuthorizationProvider {
  def checkPrivileges(spark: SparkSession, requests: Seq[AuthorizationRequest]): Seq[Boolean]

  /**
    *
    * @param spark
    * @param request
    * @throws HiveAccessControlException
    */
  def checkPrivileges(spark: SparkSession, request: AuthorizationRequest): Unit
}

object AuthorizationProvider extends Logging {
  private val propKey = "spark.sql.authorization.provider"
  // scalastyle:off
  private val defaultProviderClassName = "org.apache.spark.sql.authorization.HiveV2AuthorizationProvider"
  // scalastyle:on

  private lazy val provider: AuthorizationProvider = {
    val spark: SparkSession = SparkSession.getActiveSession
        .getOrElse(SparkSession.getDefaultSession.get)
    val clazzName = spark.conf.get(propKey, defaultProviderClassName)
    logInfo(s"Use Class: ${clazzName} as Spark SQL authorization provider")
    val clazz = Utils.classForName(clazzName)
    clazz.newInstance().asInstanceOf[AuthorizationProvider]
  }

  def checkPrivileges(spark: SparkSession, requests: Seq[AuthorizationRequest]): Seq[Boolean] = {
    provider.checkPrivileges(spark, requests)
  }

  def checkPrivileges(spark: SparkSession, request: AuthorizationRequest): Unit = {
    provider.checkPrivileges(spark, request)
  }
}