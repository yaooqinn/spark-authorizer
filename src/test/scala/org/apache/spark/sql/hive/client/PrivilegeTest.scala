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

import java.io.FileReader
import java.util

import com.google.gson.GsonBuilder
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizer
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.model.RangerPolicy.{RangerPolicyItem, RangerPolicyItemAccess, RangerPolicyResource}
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.ranger.plugin.util.ServicePolicies
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.hive.AuthzUtils
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._


class PrivilegeTest extends SparkFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }


  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .config(
        "spark.sql.extensions",
        "org.apache.ranger.authorization.spark.authorizer.RangerSparkSQLExtension"
      )
      .master("local[1]")
      .appName("SparkTest")
      .enableHiveSupport()
      .getOrCreate()

  }

  override protected def afterAll(): Unit = {
  }

  def policy(user: String,
    database: String,
    table: String = "*",
    privileges: Seq[String] = Seq("all")
  ): RangerPolicy = {
    val policy = new RangerPolicy()
    val resources = new util.HashMap[String, RangerPolicy.RangerPolicyResource]()
    resources.put("database", new RangerPolicyResource(database))
    resources.put("table", new RangerPolicyResource(table))
    resources.put("column", new RangerPolicyResource("*"))
    policy.setResources(resources)
    val item = new RangerPolicyItem
    item.setUsers(util.Arrays.asList(user))
    val accesses = new util.LinkedList[RangerPolicyItemAccess](
      privileges.map(new RangerPolicyItemAccess(_, true)).asJava
    )
    item.setAccesses(accesses)
    policy.setPolicyItems(util.Arrays.asList(item))
    policy.setIsEnabled(true)
    policy
  }

  def withPolicies(policies: RangerPolicy*)(f: => Unit): Unit = {
    val gson = (new GsonBuilder).setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting.create
    val reader = new FileReader(getTestResourcePath("test_policies.json"))
    val svcPolicies = gson.fromJson(reader, classOf[ServicePolicies])

    var id = 1L
    for (p <- policies) {
      p.setService(svcPolicies.getServiceName)
      p.setName(s"policy_${id}")
      p.setId(id)
      id = id + 1
    }
    svcPolicies.setPolicies(policies.asJava)
    val authorizer = AuthzImpl.getCachedHiveClient(spark)
      .state.getAuthorizerV2.asInstanceOf[RangerHiveAuthorizer]
    val rangerPlugin = AuthzUtils.getFieldVal(
      authorizer, "hivePlugin"
    ).asInstanceOf[RangerBasePlugin]
    rangerPlugin.setPolicies(svcPolicies)

    try f finally {
      //      AuthzUtils.setFieldVal(rangerPlugin, "policyEngine", oldPolicyEngine)
    }
  }


  private def asUser(userName: String)(f: => Unit) = {
    val user = AuthzUtils.getFieldVal(
      UserGroupInformation.getLoginUser,
      "user"
    )
    val originUser = UserGroupInformation.getLoginUser.getShortUserName
    AuthzUtils.setFieldVal(user, "shortName", userName)
    try f finally {
      AuthzUtils.setFieldVal(user, "shortName", originUser)
    }
  }

  private def withDatabase(database: String)(f: => Unit) = {
    val originDB = spark.catalog.currentDatabase
    spark.sql(s"create database if not exists ${
      database
    }")
    spark.catalog.setCurrentDatabase(database)
    try f finally {
      spark.sql(s"drop database if exists ${
        database
      }")
      spark.catalog.setCurrentDatabase(originDB)
    }
  }

  private def withTables(tables: String*)(f: => Unit): Unit = {
    tables.foreach { table =>
      spark.sql(s"create table if not exists ${
        table
      }(x INT, y STRING)")
      spark.sql(s"INSERT INTO ${table} VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    }

    try f finally {
      tables.foreach { table =>
        spark.sql(s"drop table if exists ${
          table
        }")
      }
    }
  }

  def assertPermissionDenied(f: => Unit): Unit = {
    val caught = intercept[HiveAccessControlException] {
      f
      assert(false)
    }
    caught.getMessage.startsWith("Permission denied")
  }


  test("Create database without privilege") {
    withPolicies(
      policy("root", "*", "*", Seq("all"))
    ) {
      asUser("u1") {
        assertPermissionDenied {
          spark.sql("create database d1")
        }
      }
    }
  }

  test("Create database and table with privilege") {
    withPolicies(
      policy("root", "*", "*", Seq("all")),
      policy("u1", "d1", "*", Seq("create", "all")),
      policy("u2", "d1", "*", Seq("select"))
    ) {
      asUser("u1") {
        withDatabase("d1") {
          withTables("t1", "t2") {
            asUser("u2") {
              spark.sql("select * from t1").show(false)
              spark.sql("INSERT INTO t1 VALUES (4, 'four')").show(false)
              assertPermissionDenied {
                spark.sql("drop table t1").show(false)
              }
              assertPermissionDenied {
                spark.sql("alter table t1 add columns (z INT)").show(false)
              }
              assertPermissionDenied {
                spark.sql("alter table t1 rename to t2").show(false)
              }
            }
          }
        }
      }
    }
  }

  // In order to run this test, we must add the below option to JVM option.
  // see README.md
  test("show databases") {
    withPolicies(
      policy("root", "*", "*"),
      policy("u1", "d1", "t1", Seq("select"))
    ) {
      asUser("root") {
        withDatabase("d1") {
          asUser("u1") {
            val ret = spark.sql("show databases")
            assert(ret.count() === 1)
            ret.show(false)
          }
        }
      }
    }
  }

  // In order to run this test, we must add the below option to JVM option.
  // see README.md
  test("show tables") {
    withPolicies(
      policy("root", "*", "*"),
      policy("u1", "d1", "t1", Seq("select"))
    ) {
      asUser("root") {
        withDatabase("d1") {
          withTables("t1", "t2") {
            asUser("u1") {
              val ret = spark.sql("show tables")
              assert(ret.count() === 1)
              ret.show(false)
            }
          }
        }
      }
    }
  }

}
