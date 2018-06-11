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

import java.util.{List => JList}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAccessControlException, HiveAuthzContext, HiveOperationType, HivePrivilegeObject}

import org.apache.spark.SparkConf

class SecuredHiveClientImpl(
    override val version: HiveVersion,
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    extraConfig: Map[String, String],
    initClassLoader: ClassLoader,
    override val clientLoader: IsolatedClientLoader)
  extends HiveClientImpl(
    version, sparkConf, hadoopConf, extraConfig, initClassLoader, clientLoader) {

  def checkPrivileges(
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = {

      Option(state.getAuthorizerV2) match {
        case Some(authz) =>
          try {
            authz.checkPrivileges(hiveOpType, inputObjs, outputObjs, context)
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
    }
}
