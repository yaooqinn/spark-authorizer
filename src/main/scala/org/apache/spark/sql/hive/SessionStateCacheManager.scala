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

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.Logging
import org.apache.spark.util.ShutdownHookManager

class SessionStateCacheManager(conf: Configuration) extends Logging {
  private[this] val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private[this] val userToState = new ConcurrentHashMap[String, SessionState]

  private[this] val userLastActive = new ConcurrentHashMap[String, Long]

  private[this] def currentTime: Long = System.currentTimeMillis()

  private[this] val stateCleaner = new Runnable {
    override def run(): Unit = {
      userToState.asScala.foreach {
        case (user, state) =>
          val lastActive = userLastActive.getOrDefault(user, currentTime)
          if (currentTime - lastActive > 10 * 60 * 1000) {
            userToState.remove(user)
            state.close()
            userLastActive.remove(user)
          }
        case _ =>
      }
    }
  }

  def getState(user: String): SessionState = {
    userLastActive.put(user, currentTime)
    userToState.getOrDefault(user, newState(user))
  }

  /**
   * Create a Hive [[SessionState]]
   * @return
   */
  private[this] def newState(user: String): SessionState = {
    try {
      val hiveConf = new HiveConf(conf, classOf[SessionState])
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SESSION_HISTORY_ENABLED, false)
      val state = new SessionState(hiveConf, user)
      SessionState.start(state)
      state.setIsHiveServerQuery(true)
      userToState.put(user, state)
    } catch {
      case e: RuntimeException =>
        logError("Failed to initialize SessionState for authorization", e.getCause)
        null
    }
  }

  def start(): Unit = {
    val interval = 60
    logInfo(s"Scheduling SessionState cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(stateCleaner, interval, interval, TimeUnit.SECONDS)
    ShutdownHookManager.addShutdownHook(() => this.stop())
  }

  def stop(): Unit = {
    logInfo("Stopping SessionState Cache Manager")
    cacheManager.shutdown()
    userToState.asScala.values.foreach(_.close())
    userToState.clear()
    userLastActive.clear()
  }
}

object SessionStateCacheManager {

  private[this] var manager: SessionStateCacheManager = _

  def startCacheManager(conf: Configuration): SessionStateCacheManager = {
    manager = new SessionStateCacheManager(conf)
    manager.start()
    manager
  }
}
