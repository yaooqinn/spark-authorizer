# Spark SQL Ranger Security Support Guide

Ranger security support is one of the available Authorization methods for Spark SQL with [spark-authorizer](https://github.com/yaooqinn/spark-authorizer).

Ranger is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform. The [[spark-authorizer](https://github.com/yaooqinn/spark-authorizer) enables Spark SQL with control access ability reusing [Ranger Plugin for Hive MetaStore
](https://cwiki.apache.org/confluence/display/RANGER/Ranger+Plugin+for+Hive+MetaStore). [Ranger](https://ranger.apache.org/) makes the scope of existing SQL-Standard Based Authorization expanded but without supporting Spark SQL. [spark-authorizer](https://github.com/yaooqinn/spark-authorizer) sticks them together.

## Ranger Security Support

|Configuration| Configuration File|Example| Descriptions |   
|---|---|---|---|   
|ranger.plugin.hive.policy.rest.url|ranger-hive-security.xml| http://ranger.admin.one:6080,http://ranger.admin.two.lt.163.org:6080| Comma separated list of ranger admin address|    
|ranger.plugin.hive.service.name|ranger-hive-security.xml||Name of the Ranger service containing policies for this YARN instance|    
|ranger.plugin.hive.policy.cache.dir|ranger-hive-security.xml|policycache| local cache directory for ranger policy caches|

Create `ranger-hive-security.xml` in `$SPARK_HOME/conf` with configurations above properly set.

## Configure Hive Metastore Client Side
```xml
 <!-- Ranger -->
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
        <value>
            hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager
        </value>
    </property>
```  

Add configurations above in `$SPARK_HOME/conf/hive-site.xml` to enable Ranger security support.

## Ranger Audit Support

All access to Spark SQL/Hive tables that is authorized by Ranger is automatically audited by Ranger. Auditing destination of HDFS or Solr etc is supported. 

|Configuration| Configuration File|Example| Descriptions |   
|---|---|---|---|
|xasecure.audit.is.enabled|ranger-hive-audit.xml|false|When true, auditing is enabled|
|xasecure.audit.jpa.javax.persistence.jdbc.driver|ranger-hive-audit.xml|com.mysql.jdbc.Driver|jdbc driver for audit to a mysql database destination|
|xasecure.audit.jpa.javax.persistence.jdbc.url|ranger-hive-audit.xml| jdbc:mysql://address/dbname|database instance auditing to|    
|xasecure.audit.jpa.javax.persistence.jdbc.user|ranger-hive-audit.xml|*username*|user name|     
|xasecure.audit.jpa.javax.persistence.jdbc.password|ranger-hive-audit.xml|*Password*| Password|

Create `ranger-hive-security.xml` in `$SPARK_HOME/conf` with configurations above properly set to enable or disable auditing.


## Install `ranger-hive-plugin` for Spark SQL

Please refer to the [​Install and Enable Ranger Hive Plugin](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/bk_command-line-installation/content/ch14s05s03s02.html) for an overview on how to setup Ranger jars for Spark SQL.

