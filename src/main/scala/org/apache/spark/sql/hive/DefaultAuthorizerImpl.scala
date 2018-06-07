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

package org.apache.spark.sql.hive

import java.util.{List => JList}

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAccessControlException, HiveAuthzContext, HiveOperationType, HivePrivilegeObject}
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.sql.{Logging, SparkSession}

/**
 * Default Authorizer implementation.
 *
 * The [[SessionState]] generates the authorizer and authenticator, we use these to check
 * the privileges of a Spark LogicalPlan, which is mapped to hive privilege objects and operation
 * type.
 *
 * [[SparkSession]] with hive catalog implemented has its own instance of [[SessionState]]. I am
 * strongly willing to reuse it, but for the reason that it belongs to an isolated classloader
 * which makes it unreachable for us to visit it in Spark's context classloader.
 *
 * Also, this will cause some issues with two [[SessionState]] instances in your application, such
 * as more mysql connections.
 *
 */
class DefaultAuthorizerImpl extends Logging {
  private[this] def conf =
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).get.sparkContext.getConf

  SessionStateCacheManager.startCacheManager(conf)

  /**
   * Only a given [[SparkSession]] backed by Hive will involve privilege checking
   * @return
   */
  private[this] val isHiveSessionState: Boolean =
    conf.getOption("spark.sql.catalogImplementation").exists(_.equals("hive"))

  def checkPrivileges(
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = if (isHiveSessionState) {
    val s = SessionStateCacheManager.get().getState
    if (s != null) {
      Option(s.getAuthorizerV2) match {
        case Some(authz) =>
          try {
            authz.checkPrivileges(hiveOpType, inputObjs, outputObjs, context)
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
        case None =>
          warn("Authorizer V2 not configured.")
      }
    }
  }
}

