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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
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
 * PS: For Spark 2.3 and later, because I've committed a PR to Apache Spark master branch which has
 * an side effect to expose a interface to reach it, I am able to make the inner [[SessionState]]
 * be reused.
 */
private[sql] object SessionStateOfHive {

  private def sparkSession =
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).get

  private def hadoopConf = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    Seq("hive-site.xml", "ranger-hive-security.xml", "ranger-hive-audit.xml.xml")
      .foreach { file =>
        Option(Utils.getContextOrSparkClassLoader.getResource(file)).foreach(conf.addResource)
      }
    conf
  }

  /**
   * Only a given [[SparkSession]] backed by Hive will involve privilege checking
   * @return
   */
  private def isHiveSessionState: Boolean = {
    sparkSession.sessionState.isInstanceOf[HiveSessionState]
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

  def apply(): Option[SessionState] = state
}
