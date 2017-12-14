
# Spark Authorizer

**Spark Authorizer** provides you with *SQL Standard ased Authorization* for [Apache Spark™](http://spark.apache.org) 
like [SQL Standard Based Hive Authorization](https://cwiki.apache.org/confluence/display/Hive/SQL+Standard+Based+Hive+Authorization). 
While you are using Spark SQL or Dataset/DataFrame API to load data from tables embedded with [Apache Hive™](https://hive.apache.org) metastore, 
this library provides row/column level fine-grained access controls with [Apache Ranger™](https://ranger.apache.org).

Security is one of fundamental features for enterprise adoption. [Apache Ranger™](https://ranger.apache.org) offers many security plugins for many Hadoop ecosystem components, 
such as HDFS, Hive, HBase, Solr and Sqoop2. However, [Apache Spark™](http://spark.apache.org) is not counted in yet. 
When a secured HDFS cluster is used as a data warehouse accessed by various users and groups via different applications wrote by Spark and Hive, 
it is very difficult to guarantee data management in a consistent way.  Apache Spark users visit data warehouse only 
with Storage based access controls offered by HDFS. This library shares [Ranger Hive plugin](https://cwiki.apache.org/confluence/display/RANGER/Apache+Ranger+0.5.0+Installation#ApacheRanger0.5.0Installation-InstallingApacheHive(1.2.0)) 
with Hive to help Spark talking to Ranger Admin. 

---

## Building Spark Authorizer

Spark Authorizer is built using [Apache Maven](http://maven.apache.org). To build it, run:

```bash
git clone https://github.com/yaooqinn/spark-authorizer.git
cd spark-authorizer
# choose a branch of your spark version
git checkout spark-<spark.branch.version>
mvn package
```

## Specifying Spark Authorization for Apache Spark

|Branch| Spark Version| Notes|
|:---:|:---:|:---:|
|master|master|periodically update to catch up|
|spark-2.2|2.2.1| - |
|spark-2.1|2.1.2| - |

---

## Installing Spark Authorizer to Spark

  1. `cp spark-authorizer-<version>.jar $SPARK_HOME/jars`
  2. install ranger-hive-plugin for spark
  3. configure you `hive-site.xml` and ranger configuration file, you may find an sample in `[./conf]`

---

## Interactive Spark Shell

The easiest way to start using Spark is through the Scala shell:

```shell
bin/spark-shell --master yarn --proxy-user hzyaoqin
```

Secondly, implement the Authorizer Rule to Spark's extra Optimizations.

```scala
import org.apache.spark.sql.catalyst.optimizer.Authorizer
```

```scala
spark.experimental.extraOptimizations ++= Seq(Authorizer)
```

Check it out
```scala
scala> spark.experimental.extraOptimizations
res2: Seq[org.apache.spark.sql.catalyst.rules.Rule[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]] = List(org.apache.spark.sql.catalyst.optimizer.Authorizer$@1196537d)
```

Note that extra optimizations are appended to the end of all the inner optimizing rules.
It's good for us to do authorization after column pruning.

Your may notice that it only shut the door for men with a noble character but leave the door open for the scheming ones.

To avoid that, I suggest you modify [ExperimentalMethods.scala#L47](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/ExperimentalMethods.scala#L47) and [Bulid Spark](http://spark.apache.org/docs/latest/building-spark.html) of your own.


```scala
@volatile var extraOptimizations: Seq[Rule[LogicalPlan]] = Nil
```
to

```scala
@volatile val extraOptimizations: Seq[Rule[LogicalPlan]] = Seq(Authorizer)

```

Make extraOptimizations to a `val` to avoid reassignment.

Without modifying, you either control the spark session such as supplying a Thrift/JDBC Sever or hope for "Manner maketh Man"

---

## Suffer for the Authorization Pain

We create a ranger policy as below:

![ranger-prolcy-details](./img/ranger-prolcy-details.png)

Check Privilage with some simple cases.

### show database

Actually, user [hzyaoqin] show only see only one database -- tpcds_10g_ext, this is not a bug, but a compromise not hacking 
```sql
scala> spark.sql("show databases").show
+--------------+
|  databaseName|
+--------------+
|       default|
| spark_test_db|
| tpcds_10g_ext|
+--------------+
```

### switch database

```sql
scala> spark.sql("use spark_test_db").show
17/12/08 17:06:17 ERROR optimizer.Authorizer:
+===============================+
|Spark SQL Authorization Failure|
|-------------------------------|
|Permission denied: user [hzyaoqin] does not have [USE] privilege on [spark_test_db]
|-------------------------------|
|Spark SQL Authorization Failure|
+===============================+
```
Oops...


```sql
scala> spark.sql("use tpcds_10g_ext").show
++
||
++
++
```
LOL...


### select 
```sql
scala> spark.sql("select cp_type from catalog_page limit 1").show
17/12/08 17:09:58 ERROR optimizer.Authorizer:
+===============================+
|Spark SQL Authorization Failure|
|-------------------------------|
|Permission denied: user [hzyaoqin] does not have [SELECT] privilege on [tpcds_10g_ext/catalog_page/cp_type]
|-------------------------------|
|Spark SQL Authorization Failure|
+===============================+
```
Oops...

```sql
scala> spark.sql("select * from call_center limit 1").show
+-----------------+-----------------+-----------------+---------------+-----------------+---------------+--------+--------+------------+--------+--------+-----------+---------+--------------------+--------------------+-----------------+-----------+----------------+----------+---------------+----------------+--------------+--------------+---------------+-------+-----------------+--------+------+-------------+-------------+-----------------+
|cc_call_center_sk|cc_call_center_id|cc_rec_start_date|cc_rec_end_date|cc_closed_date_sk|cc_open_date_sk| cc_name|cc_class|cc_employees|cc_sq_ft|cc_hours| cc_manager|cc_mkt_id|        cc_mkt_class|         cc_mkt_desc|cc_market_manager|cc_division|cc_division_name|cc_company|cc_company_name|cc_street_number|cc_street_name|cc_street_type|cc_suite_number|cc_city|        cc_county|cc_state|cc_zip|   cc_country|cc_gmt_offset|cc_tax_percentage|
+-----------------+-----------------+-----------------+---------------+-----------------+---------------+--------+--------+------------+--------+--------+-----------+---------+--------------------+--------------------+-----------------+-----------+----------------+----------+---------------+----------------+--------------+--------------+---------------+-------+-----------------+--------+------+-------------+-------------+-----------------+
|                1| AAAAAAAABAAAAAAA|       1998-01-01|           null|             null|        2450952|NY Metro|   large|           2|    1138| 8AM-4PM|Bob Belcher|        6|More than other a...|Shared others cou...|      Julius Tran|          3|             pri|         6|          cally|             730|      Ash Hill|     Boulevard|        Suite 0| Midway|Williamson County|      TN| 31904|United States|        -5.00|             0.11|
+-----------------+-----------------+-----------------+---------------+-----------------+---------------+--------+--------+------------+--------+--------+-----------+---------+--------------------+--------------------+-----------------+-----------+----------------+----------+---------------+----------------+--------------+--------------+---------------+-------+-----------------+--------+------+-------------+-------------+-----------------+

```

LOL...

### Dataset/DataFrame

```scala
scala> spark.read.table("catalog_page").limit(1).collect
```
```
17/12/11 14:46:33 ERROR optimizer.Authorizer:
+===============================+
|Spark SQL Authorization Failure|
|-------------------------------|
|Permission denied: user [hzyaoqin] does not have [SELECT] privilege on [tpcds_10g_ext/catalog_page/cp_catalog_page_sk,cp_catalog_page_id,cp_promo_id,cp_start_date_sk,cp_end_date_sk,cp_department,cp_catalog_number,cp_catalog_page_number,cp_description,cp_type]
|-------------------------------|
|Spark SQL Authorization Failure|
+===============================+
```
Oops...

```scala
scala> spark.read.table("call_center").limit(1).collect
```
```
res3: Array[org.apache.spark.sql.Row] = Array([1,AAAAAAAABAAAAAAA,1998-01-01,null,null,2450952,NY Metro,large,2,1138,8AM-4PM,Bob Belcher,6,More than other authori,Shared others could not count fully dollars. New members ca,Julius Tran,3,pri,6,cally,730,Ash Hill,Boulevard,Suite 0,Midway,Williamson County,TN,31904,United States,-5.00,0.11])
```
LOL...

---
