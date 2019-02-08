# Spark SQL SQL-Standard Based Authorization Guide

[SQL-Standard Based Authorization](https://cwiki.apache.org/confluence/display/Hive/SQL+Standard+Based+Hive+Authorization) is one of the available Authorization methods for Spark SQL with [spark-authorizer](https://github.com/yaooqinn/spark-authorizer).

The [spark-authorizer](https://github.com/yaooqinn/spark-authorizer) can enable Spark SQL with Hive's [SQL-Standard Based Authorization](https://cwiki.apache.org/confluence/display/Hive/SQL+Standard+Based+Hive+Authorization) for fine grained access control. It is based on the SQL standard for authorization, and uses the familiar GRANT/REVOKE statements to control access.

For Spark SQL Client users who can directly access HDFS, the SQL-Standards Based Authorization can be easily bypassed.

With [Kyuubi](https://github.com/yaooqinn/kyuubi), the SQL-Standard Based Authorization is guaranteed for the security configurations, metadata, and storage informations are preserved from end users. It can be used along with storage based authorization on the `Hive Metastore Server`.

## Minimum Permissions

The following table shows the minimum permissions required for Spark SQL when using SQL-Standard Based Authorization:

Operation |  Permission required
---|---
create table  |  ownership of database
drop table  | ownership
describe table | select
show partitions | select
alter table location  |  ownership; URI privilege: RWX permission + ownership (for new location)
alter partition location  |  ownership; URI privilege: RWX permission + ownership (for new partition location)
alter table add partition  | insert; URI privilege: RWX permission + ownership (for partition location)
alter table drop partition | delete
alter table (all of them except the ones listed above) | ownership
truncate table | ownership
create view | select "with grant option"
alter view properties  | ownership
alter view rename   | ownership
drop view  | ownership
analyze Table |  select and insert
show columns   | select
show table properties  | Select
CTAS | select (of input) and ownership (of database)
select | select
insert | insert and delete (for overwrite)
delete | delete
load  |  insert (output); delete (output); URI privilege: RWX permission + ownership (input location)
show create table  | select "with grant option"
create function | admin
drop function  | admin
MSCK |admin
alter database|  admin
create database | URI privilege: RWX permission + ownership (if custom location specified)
explain | select
drop database |  ownership


## Privileges
Select - READ access to an object  
Insert - access to ADD data to an object (table)  
Update - access to run UPDATE queries on an object (table)  
Delete - access to DELETE data in an object (table)  
All Privileges - includes all above privileges


## Limitations

Spark SQL does not support grant/revoke statement, which might be done only in Hive.
