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

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.hive.ql.security.authorization.plugin._
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
object AuthorizerImpl extends Logging {

  def checkPrivileges(
      client: HiveClient,
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = {
    val clientLoader = getFiledVal(client, "clientLoader").asInstanceOf[IsolatedClientLoader]
    val originClassLoader = Thread.currentThread().getContextClassLoader
    val hiveClassLoader = clientLoader.classLoader
    try {
      Thread.currentThread().setContextClassLoader(hiveClassLoader)
      val state = getFiledVal(client, "state")
      val authzV2 = invoke(state, "getAuthorizerV2", Seq.empty, Seq.empty)
      val authz = authzV2.asInstanceOf[HiveAuthorizer]
      if (authz != null) {
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

      } else {
        warn("Authorizer V2 not configured. Skipping privilege checking")
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader)
    }
  }

  def getFiledVal(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  /**
   * Invoke a method of an object via reflection
   * @param o object
   * @param name method name
   * @param argTypes arguments class type
   * @param params arguments object list
   * @return
   */
  def invoke(o: Any, name: String, argTypes: Seq[Class[_]], params: Seq[AnyRef]): Any = {
    require(o != null, "object could not be null!")
    Try {
      val method = o.getClass.getDeclaredMethod(name, argTypes: _*)
      method.setAccessible(true)
      method.invoke(o, params: _*)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }
}

