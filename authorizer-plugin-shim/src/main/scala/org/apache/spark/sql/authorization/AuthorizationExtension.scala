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

// scalastyle:off
package org.apache.spark.sql.authorization

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.classloader.SparkPluginClassLoader
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule


class AuthorizationExtension extends (SparkSessionExtensions => Unit) with Logging {

  private val SPARK_PLUGIN_TYPE = "authorizer"
  private val AUTHORIZER_IMPL_CLASSNAME = "org.apache.spark.sql.catalyst.optimizer.AuthorizerExtension"

  private lazy val pluginClassLoader = SparkPluginClassLoader.getInstance(SPARK_PLUGIN_TYPE, this.getClass)

  private lazy val authorizerImpl: SparkSession => Rule[LogicalPlan] = {
    logDebug("==> AuthorizationExtension.init()")
    try {
      val cls = Class.forName(AUTHORIZER_IMPL_CLASSNAME + "$", true, pluginClassLoader)
      activatePluginClassLoader()
      val instance = cls.getField("MODULE$").get(cls).asInstanceOf[SparkSession => Rule[LogicalPlan]]
      logDebug("<== AuthorizationExtension.init()")
      instance
    } catch {
      case e: Exception => {
        // check what need to be done
        logError("Error Enabling AuthorizationExtension", e)
        throw e
      }
    } finally deactivatePluginClassLoader()
  }


  private def activatePluginClassLoader(): Unit = {
    if (pluginClassLoader != null) pluginClassLoader.activate
  }

  private def deactivatePluginClassLoader(): Unit = {
    if (pluginClassLoader != null) pluginClassLoader.deactivate
  }

  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectOptimizerRule(authorizerImpl)
  }


}
