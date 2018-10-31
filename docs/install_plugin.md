# Installing Apache Ranger Hive Plugin For Apache Spark

This article illustrates how to install the Apache Ranger plugin which is made for Apache Hive to Apache Spark with [spark-authorizer](https://github.com/yaooqinn/spark-authorizer). We guarantee column/row level fine gained [ACL Management for Spark SQL](https://yaooqinn.github.io/spark-authorizer/docs/spark_sql_authorization.html).

Apache Spark is built bundled with built-in Hive Metastore client(version 1.2.1.spark2) jars when `-Phive` is enabled. AFAIK, this version of Hive Metastore client is compatible with all Hive Metastore server 1.2.1 and higher versions.

We **DO NOT** support configuring `spark.sql.hive.metastore.jars` to `maven` or a location of the jars used to instantiate the `HiveMetastoreClient`, `builtin` is the one and only option.

Apache Ranger upgrades quite fast, one of the reasons may be to catch up with the higher Hive releases. AFAIK, Apache Ranger 0.6.x and higher versions do not support [1.2.1](https://issues.apache.org/jira/browse/RANGER-1056) anymore, accordingly, you may use Apache Ranger 0.5.x to avoid underlying pitfalls.

An official installation guide of Apache Ranger 0.5.x can be found [here](https://cwiki.apache.org/confluence/display/RANGER/Apache+Ranger+0.5.0+Installation). The remainder of this article will guide you how to install Apache Ranger Hive Plugin for Apache Spark, which is not mentioned in the official documentation.

## Building Apache Ranger

1. git clone git@github.com:apache/ranger.git
2. cd ranger
3. git fetch https://github.com/apache/ranger.git ranger-0.5.3-rc3:ranger-0.5.3
4. git checkout ranger-0.5.3
5. mvn clean compile package assembly:assembly install -Dmaven.test.skip=true

If you failed to build the project, please refer to the instructions of the official doc to see if there are any prerequisites.

If you successfully make the mvn command work, all archives of Ranger admin and plugins will be generated in `./target`, including `ranger-0.5.3-hive-plugin.tar.gz` which is exactly you need for next steps
```
-rw-r--r-- 1 kent hadoop 163667362 Oct 15 15:38 ranger-0.5.3-admin.tar.gz
-rw-r--r-- 1 kent hadoop 164655504 Oct 15 15:38 ranger-0.5.3-admin.zip
-rw-r--r-- 1 kent hadoop  16771141 Oct 15 15:36 ranger-0.5.3-hbase-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  16797394 Oct 15 15:36 ranger-0.5.3-hbase-plugin.zip
-rw-r--r-- 1 kent hadoop  16722944 Oct 15 15:35 ranger-0.5.3-hdfs-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  16747829 Oct 15 15:35 ranger-0.5.3-hdfs-plugin.zip
-rw-r--r-- 1 kent hadoop  16139126 Oct 15 15:35 ranger-0.5.3-hive-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  16165266 Oct 15 15:36 ranger-0.5.3-hive-plugin.zip
-rw-r--r-- 1 kent hadoop  32975495 Oct 15 15:36 ranger-0.5.3-kafka-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  33012135 Oct 15 15:37 ranger-0.5.3-kafka-plugin.zip
-rw-r--r-- 1 kent hadoop  71917257 Oct 15 15:38 ranger-0.5.3-kms.tar.gz
-rw-r--r-- 1 kent hadoop  72005470 Oct 15 15:39 ranger-0.5.3-kms.zip
-rw-r--r-- 1 kent hadoop  21298145 Oct 15 15:36 ranger-0.5.3-knox-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  21322990 Oct 15 15:36 ranger-0.5.3-knox-plugin.zip
-rw-r--r-- 1 kent hadoop     34600 Oct 15 15:38 ranger-0.5.3-migration-util.tar.gz
-rw-r--r-- 1 kent hadoop     38014 Oct 15 15:38 ranger-0.5.3-migration-util.zip
-rw-r--r-- 1 kent hadoop  18485767 Oct 15 15:39 ranger-0.5.3-ranger-tools.tar.gz
-rw-r--r-- 1 kent hadoop  18495143 Oct 15 15:39 ranger-0.5.3-ranger-tools.zip
-rw-r--r-- 1 kent hadoop  22416054 Oct 15 15:37 ranger-0.5.3-solr-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  22441083 Oct 15 15:37 ranger-0.5.3-solr-plugin.zip
-rw-r--r-- 1 kent hadoop   3606416 Oct 15 15:39 ranger-0.5.3-src.tar.gz
-rw-r--r-- 1 kent hadoop   5481890 Oct 15 15:39 ranger-0.5.3-src.zip
-rw-r--r-- 1 kent hadoop  34769024 Oct 15 15:36 ranger-0.5.3-storm-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  34788044 Oct 15 15:36 ranger-0.5.3-storm-plugin.zip
-rw-r--r-- 1 kent hadoop  13512547 Oct 15 15:38 ranger-0.5.3-usersync.tar.gz
-rw-r--r-- 1 kent hadoop  13534930 Oct 15 15:38 ranger-0.5.3-usersync.zip
-rw-r--r-- 1 kent hadoop  15942158 Oct 15 15:37 ranger-0.5.3-yarn-plugin.tar.gz
-rw-r--r-- 1 kent hadoop  15969320 Oct 15 15:37 ranger-0.5.3-yarn-plugin.zip
```

## Applying Plugin to Apache Spark

1. cd target
2. tar zxf ranger-0.5.3-hive-plugin.tar.gz
3. cd ranger-0.5.3-hive-plugin/lib

```
drwxr-xr-x 2 kent hadoop  4096 Oct 16 12:34 ranger-hive-plugin-impl
-rw-r--r-- 1 kent hadoop 16061 Oct 15 15:35 ranger-hive-plugin-shim-0.5.3.jar
-rw-r--r-- 1 kent hadoop 16545 Oct 15 15:35 ranger-plugin-classloader-0.5.3.jar
```

Fistly, copy the above folds and files to `$SPARK_HOME/jars`

```
jersey-client-1.9.jar
jersey-bundle-1.4.jar
eclipselink-2.5.2.jar
noggit-0.6.jar
gson-2.2.4.jar
httpclient-4.5.3.jar
httpcore-4.4.6.jar
httpmime-4.5.3.jar
javax.persistence-2.1.0.jar
mybatis-3.2.8.jar
mysql-connector-java-5.1.39.jar
```

Secondly, add the jars listed above to `$SPARK_HOME/jars/ranger-hive-plugin-impl/` if missing.

## Configuring Ranger for Apache Spark


Firstly, add the following configurations in `hive-site.xml` to enable Ranger Authorization.

```
<property>
    <name>hive.security.authorization.enabled</name>
    <value>true</value>
</property>

<property>
    <name>hive.security.authorization.manager</name>
    <value>org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory</value>
</property>

<property>
    <name>hive.security.authenticator.manager</name>
    <value>org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator</value>
</property>

<property>
    <name>hive.conf.restricted.list</name>
    <value>hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager</value>
</property>
```

Secondly, create `ranger-hive-security.xml` in `$SPARK_HOME/conf` and add the following configurations for pointing to the right ranger admin server.

```
<configuration>

    <property>
        <name>ranger.plugin.hive.policy.rest.url</name>
        <value>ranger admin address like http://ranger-admin.org:6080</value>
    </property>

    <property>
        <name>ranger.plugin.hive.service.name</name>
        <value>a ranger hive service name</value>
    </property>

    <property>
        <name>ranger.plugin.hive.policy.cache.dir</name>
        <value>./a ranger hive service name/policycache</value>
    </property>

    <property>
        <name>ranger.plugin.hive.policy.pollIntervalMs</name>
        <value>5000</value>
    </property>

    <property>
        <name>ranger.plugin.hive.policy.source.impl</name>
        <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
    </property>

</configuration>
```

At last, create create `ranger-hive-audit.xml` in `$SPARK_HOME/conf` and add the following configurations to enable/disable auditing.

```
<configuration>

    <property>
        <name>xasecure.audit.is.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db</name>
        <value>false</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.jdbc.driver</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.jdbc.url</name>
        <value>jdbc:mysql://10.171.161.78/ranger</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.password</name>
        <value>rangeradmin</value>
    </property>

    <property>
        <name>xasecure.audit.destination.db.user</name>
        <value>rangeradmin</value>
    </property>

</configuration>
```

Ranger Hive plugins should work well through `spark-authorizer`, when set `spark.sql.extensions`=`org.apache.ranger.authorization.spark.authorizer.RangerSparkSQLExtension`
## Additional Notes

If you are using Apache Spark in `cluster` mode, the jar files under `$SPARK_HOME/jars/ranger-hive-plugin-impl/` will not be uploaded automatically. If you are not familiar with Spark source code and unable to make some modifications, I suggest you copy all files in `$SPARK_HOME/jars/ranger-hive-plugin-impl/` to `$SPARK_HOME/jars/` and **DELETE** `ranger-hive-plugin-shim-0.5.3.jar` in `$SPARK_HOME/jars/`. This works fine for your whole Spark application but with a tiny problem of Spark UI because of jersey jars confliction.

Also for `cluster` mode Spark applications, `ranger.plugin.hive.policy.cache.dir` in `ranger-hive-security.xml` must be able to create on all NodeManager nodes for the Spark Driver could be generated anywhere. One convenient and effective way is to configure the relative path, such as,
```
<property>
    <name>ranger.plugin.hive.policy.cache.dir</name>
    <value>policycache</value>
 </property>
```
