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
import java.util.concurrent.locks.{Lock, ReentrantLock}

import com.githup.yaooqinn.spark.authorizer.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.security.authorization.plugin._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{AuthzUtils, HiveUtils}
import org.apache.spark.util.Utils

/**
 * A Tool for Authorizer implementation.
 *
 * The [[SessionState]] generates the authorizer and authenticator, we use these to check
 * the privileges of a Spark LogicalPlan, which is mapped to hive privilege objects and operation
 * type.
 *
 * [[SparkSession]] with hive catalog implemented has its own instance of [[SessionState]]. I am
 * strongly willing to reuse it, but for the reason that it belongs to an isolated classloader
 * which makes it unreachable for us to visit it in Spark's context classloader. So, when
 * [[ClassCastException]] occurs, we turn off [[IsolatedClientLoader]] to use Spark's builtin
 * Hive client jars to generate a new metastore client to replace the original one, once it is
 * generated, will be reused then.
 *
 */
object AuthzImpl extends Logging {

  private var _cachedHiveClient: HiveClientImpl = null
  private val cachedHiveClientLock: Lock = new ReentrantLock()


  private[client] lazy val extraConfig: Map[String, String] = {

    var classLoader = Thread.currentThread.getContextClassLoader
    if (classLoader == null) classLoader = AuthzImpl.getClass.getClassLoader
    val confURL = classLoader.getResource("hive-security.xml")
    if (confURL != null) {
      val conf = new Configuration(false)
      info(s"Read extra configurations from file `${confURL.getPath}`")
      conf.addResource(confURL)
      val keys = Seq("hive.security.authorization.enabled",
        "hive.security.authorization.manager",
        "hive.security.authenticator.manager",
        "hive.conf.restricted.list")
      // Because the hive client use for authorization do need meta data store in the metastore,
      // so we config this client to use a new in-memory Derby database here to avoid multi
      // connections exception in using embedded derby database
      var config = HiveUtils.newTemporaryConfiguration(true)
      for(key <- keys) {
        val value: String = conf.get(key)
        if(value != null) {
          config = config + (key -> conf.get(key))
          info(s"Get extra config: ${key}=${value}")
        }
      }
      config
    } else {
      Map.empty
    }
  }


  private [sql] def getCachedHiveClient(spark: SparkSession): HiveClientImpl = {
    if (_cachedHiveClient == null) {
      cachedHiveClientLock.lock()
      if (_cachedHiveClient != null) return _cachedHiveClient
      _cachedHiveClient = newClientForPrivilegeCheck(spark)
      cachedHiveClientLock.unlock()
    }
    return _cachedHiveClient
  }

  /**
    * Create a [[HiveClient]] used for authorization.
    *
    * Currently this must always be Hive 13 as this is the version of Hive that is packaged
    * with Spark SQL.
    */
  protected[hive] def newClientForPrivilegeCheck(spark: SparkSession): HiveClientImpl = {

    val builtinHiveVersion: String = try {
      // spark 2.2
      AuthzUtils.getFieldVal(HiveUtils, "hiveExecutionVersion").toString
    } catch {
      case _: NoSuchFieldException =>
        // spark 2.3
        AuthzUtils.getFieldVal(HiveUtils, "builtinHiveVersion").toString
    }
    info(s"Initializing privilege check hive, version $builtinHiveVersion")



    // the new client may use current thread local SessionState as its state, so we need to
    // detach current thread local session in order to get a new state
    SessionState.detachSession()
    val loader = new IsolatedClientLoader(
      version = IsolatedClientLoader.hiveVersion(builtinHiveVersion),
      sparkConf = spark.sqlContext.sparkContext.conf,
      execJars = Seq.empty,
      hadoopConf = spark.sqlContext.sessionState.newHadoopConf(),
      config = extraConfig,
      isolationOn = false,
      baseClassLoader = Utils.getContextOrSparkClassLoader)
    val c = loader.createClient().asInstanceOf[HiveClientImpl]
    // make sure we get a new state
    val _ = c.state
    c
  }

  def checkPrivileges(
      spark: SparkSession,
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = {

    val clientImpl = getCachedHiveClient(spark)

    val state = clientImpl.state
    SessionState.setCurrentSessionState(state)
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    if (state.getAuthenticator.getUserName != user) {
      val hiveConf = state.getConf
      val newState = new SessionState(hiveConf, user)
      SessionState.start(newState)
      AuthzUtils.setFieldVal(clientImpl, "state", newState)
    }

    val authz = clientImpl.state.getAuthorizerV2
    clientImpl.withHiveState {
      if (authz != null) {
          authz.checkPrivileges(hiveOpType, inputObjs, outputObjs, context)
      } else {
        warn("Authorizer V2 not configured. Skipping privilege checking")
      }
    }
  }
}
