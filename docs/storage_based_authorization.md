# Spark SQL Storage-Based Authorization Guide

Storage-Based Authorization is one of the available Authorization methods for Spark SQL with or without [spark-authorizer](https://github.com/yaooqinn/spark-authorizer).

When the `Hive Metastore Server` is configured to use Storage-Based Authorization, it uses the file system permissions for directories corresponding to the different kinds of metadata objects as the source of verification for the authorization policy. Using this authorization method is recommended in the metastore server.

With Hive Metastore as the external catalog for Spark SQL, there is a corresponding directory to a database or table for each file system that is used at storage layer. Using this authorization model, the rwx permissions for this directory also determines the permissions of a user, or group, to the database or table.

With Hive 0.14 or onwards as Spark SQL's metastore client, this could be enabled without [spark-authorizer](https://github.com/yaooqinn/spark-authorizer)'s support.


## Configuring Parameters for Storage-Based Authorization

### Hive Metastore Server Side

To enable Storage-based Authorization in the Hive metastore server, configure these properties in the `hive-site.xml` for the server.

Configuration| Description
---|---
`hive.metastore.pre.event.listeners` | This parameter enables metastore security. Set to `org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener`
`hive.security.metastore.authenticator.manager`  | Set to `org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator`
`hive.security.metastore.authorization.auth.reads`   | When true, Hive metastore authorization checks for read access.
`hive.security.metastore.authorization.manager`  | A comma-separated list of the names of authorization manager classes that are to be used in the metastore for authorization. Set to `org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider`

### Hive Metastore Client Side

This could be Spark SQL Client, Spark Thrift Server, HiveServer2, Kyuubi etc. Configure these properties in the `hive-site.xml` for the client.

Configuration| Description
---|---
`hive.security.authorization.enabled` | Enables or disables authorization. In the Advanced hiveserver-site section, change the value to true to enable authorization for HiveServer2. In the General section, set this value to false.
`hive.security.authorization.manager` | The class name of the Hive client authorization manager. For storage-based authorization, specify the value `org.apache.hadoop.hive.ql.security. authorization.StorageBasedAuthorizationProvider`

## Minimum Permissions

The following table shows the minimum permissions required for Spark SQL when using Storage-Based Authorization:

Operation |  Permission required
---|---
alter table | table write access
create table |   database write access
CTAS |  table read access
load   | table write access
select | table read access
show tables | database read access


## Limitations

Spark SQL does not support grant/revoke statement, which might be done only in Hive.
