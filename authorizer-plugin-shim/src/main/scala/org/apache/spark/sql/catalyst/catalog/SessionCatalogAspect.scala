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

// scalastyle:off
package org.apache.spark.sql.catalyst.catalog


import java.lang.reflect.Method

import org.apache.spark.internal.Logging
import org.apache.spark.plugin.classloader.SparkPluginClassLoader
import org.apache.spark.sql.catalyst.TableIdentifier
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{Around, Aspect}


/**
 * Aspect for filtering SessionCatalog listDatabase and listTables result,
 * only return the resources that user have privileges to access to.
 */
@Aspect
class SessionCatalogAspect extends Logging {

  private val SPARK_PLUGIN_TYPE = "authorizer"
  private val REAL_IMPL_CLASSNAME = "org.apache.spark.sql.catalyst.catalog.SessionCatalogAspect"

  private val pluginClassLoader = SparkPluginClassLoader.getInstance(SPARK_PLUGIN_TYPE, this.getClass)

  private val realImpl: AnyRef = {
    logDebug("==> SessionCatalogAspect.init()")
    try {
      val cls = Class.forName(REAL_IMPL_CLASSNAME, true, pluginClassLoader)
      activatePluginClassLoader()
      val instance = cls.newInstance().asInstanceOf[AnyRef]
      logDebug("<== SessionCatalogAspect.init()")
      instance
    } catch {
      case e: Exception => {
        // check what need to be done
        logError("Error Enabling AuthorizationExtension", e)
        throw e
      }
    } finally deactivatePluginClassLoader()
  }

  private val filterListDatabasesResultMethod: Method = realImpl.getClass.getDeclaredMethod(
    "filterListDatabasesResult", classOf[ProceedingJoinPoint])
  private val filterListTablesResultMethod: Method = realImpl.getClass.getDeclaredMethod(
    "filterListTablesResult", classOf[ProceedingJoinPoint])

  private def activatePluginClassLoader(): Unit = {
    if (pluginClassLoader != null) pluginClassLoader.activate
  }

  private def deactivatePluginClassLoader(): Unit = {
    if (pluginClassLoader != null) pluginClassLoader.deactivate
  }

  @Around(
    "execution(public * org.apache.spark.sql.catalyst.catalog.SessionCatalog.listDatabases())"
  )
  def filterListDatabasesResult(pjp: ProceedingJoinPoint): Seq[String] = {
    filterListDatabasesResultMethod.invoke(realImpl, pjp).asInstanceOf[Seq[String]]
  }

  @Around(
    "execution(public * org.apache.spark.sql.catalyst.catalog.SessionCatalog.listTables(" +
      "java.lang.String, java.lang.String" +
      "))"
  )
  def filterListTablesResult(pjp: ProceedingJoinPoint): Seq[TableIdentifier] = {
    filterListTablesResultMethod.invoke(realImpl, pjp).asInstanceOf[Seq[TableIdentifier]]
  }

}
