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

import java.io.File
import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAccessControlException, HiveAuthzContext, HiveOperationType, HivePrivilegeObject}
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Authorizable, SparkSession}
import org.apache.spark.util.Utils

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
 * as more mysql connections, `show tables` command may expose all tables including unexpected one.
 *
 */
class DefaultAuthorizerImpl extends Authorizable with Logging {
  private def sparkSession =
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).get

  private def hadoopConf = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    Seq("hive-site.xml", "ranger-hive-security.xml", "ranger-hive-audit.xml.xml")
      .foreach { file =>
        Option(Utils.getContextOrSparkClassLoader.getResource(file)).foreach(conf.addResource)
      }

    val dir = conf.get("ranger.plugin.hive.policy.cache.dir")
    if (dir != null) {
      val file = new File(dir)
      if (!file.exists()) {
        if (file.mkdirs()) {
          logInfo("Creating ranger policy cache directory at " + file.getAbsolutePath)
          file.deleteOnExit()
        } else {
          logWarning("Unable to create ranger policy cache directory at "
            + file.getAbsolutePath)
        }
      } else {
        logWarning("Ranger policy cache directory already exists")
      }
    }
    conf
  }

  /**
   * Only a given [[SparkSession]] backed by Hive will involve privilege checking
   * @return
   */
  private def isHiveSessionState: Boolean = {
    sparkSession.sharedState.externalCatalog.isInstanceOf[HiveExternalCatalog]
  }

  /**
   * A Hive [[SessionState]] which holds the authenticator and authorizer
   */
  private val state: Option[SessionState] = {
    if (this.isHiveSessionState) {
      Option(SessionState.get()).orElse {
        Some(this.newState())
      }
    } else {
      None
    }
  }

  /**
   * Get SPARK_USER
   * @return
   */
  private def getCurrentUser: String = Utils.getCurrentUserName()

  /**
   * Create a Hive [[SessionState]]
   * @return
   */
  private def newState(): SessionState = {
    val hiveConf = new HiveConf(hadoopConf, classOf[SessionState])
    val state = new SessionState(hiveConf, this.getCurrentUser)
    SessionState.start(state)
    state
  }
  override def checkPrivileges(
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = {
    state.foreach { s =>
      s.setIsHiveServerQuery(true)
      Option(s.getAuthorizerV2) match {
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
      hiveOpType match {
        case HiveOperationType.SWITCHDATABASE =>
          inputObjs.asScala.foreach(o => s.setCurrentDatabase(o.getDbname))
        case _ =>
      }
    }
  }

  /**
   * get the current database name
   *
   * @return database name
   */
  override def currentDatabase(): String = {
    require(state.nonEmpty)
    state.get.getCurrentDatabase
  }
}
