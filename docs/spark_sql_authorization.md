# ACL Management for Spark SQL

Three primary modes for Spark SQL authorization are available by spark-authorizer:

## Storage-Based Authorization
    
Enabling Storage Based Authorization in the `Hive Metastore Server` uses the HDFS permissions to act as the main source for verification and allows for consistent data and metadata authorization policy. This allows control over metadata access by verifying if the user has permission to access corresponding directories on the HDFS. Similar with `HiveServer2`, files and directories will be tanslated into hive metadata objects, such as dbs, tables, partitions, and be protected from end user's queries through Spark SQL as a service like [Kyuubi](https://github.com/yaooqinn/kyuubi), livy etc.

Storage-Based Authorization offers users with Database, Table and Partition-level coarse-gained access control.

Please refer to the [Storage-Based Authorization Guide](https://yaooqinn.github.io/spark-authorizer/docs/storage_based_authorization.html) in the online documentation for an overview on how to configure Storage-Based Authorization for Spark SQL.

## SQL-Standard Based Authorization

Enabling SQL-Standard Based Authorization gives users more fine-gained control over access comparing with Storage Based Authorization. Besides of the ability of Storage Based Authorization,  SQL-Standard Based Authorization can improve it to Views and Column-level. Unfortunately, Spark SQL does not support grant/revoke statements which controls access, this might be done only through the  HiveServer2. But it's gratifying that [spark-authorizer](https://github.com/yaooqinn/spark-authorizer) makes Spark SQL be able to understand this fine-grain access control granted or revoked by Hive.

For Spark SQL Client users who can directly acess HDFS, the SQL-Standard Based Authorization can be easily bypassed.

With [Kyuubi](https://github.com/yaooqinn/kyuubi), the SQL-Standard Based Authorization is guaranteed for the security configurations, metadata, and storage information is preserved from end users.

Please refer to the [SQL-Standard Based Authorization Guide](https://yaooqinn.github.io/spark-authorizer/docs/sql_std_based_authorization.html) in the online documentation for an overview on how to configure SQL-Standard Based Authorization for Spark SQL.

## Ranger

[Apache Ranger](https://ranger.apache.org/)  is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform but end before Spark or Spark SQL. The [spark-authorizer](https://github.com/yaooqinn/spark-authorizer) enables Spark SQL with control access ability reusing [Ranger Plugin for Hive MetaStore
](https://cwiki.apache.org/confluence/display/RANGER/Ranger+Plugin+for+Hive+MetaStore). [Apache Ranger](https://ranger.apache.org/) makes the scope of existing SQL-Standard Based Authorization expanded but without supporting Spark SQL. [spark-authorizer](https://github.com/yaooqinn/spark-authorizer) sticks them together.

Please refer to the [Spark SQL Ranger Security Support Guide](https://yaooqinn.github.io/spark-authorizer/docs/ranger_authorization.html) in the online documentation for an overview on how to configure Ranger for Spark SQL.
