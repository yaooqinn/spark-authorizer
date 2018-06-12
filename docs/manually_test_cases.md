# Spark SQL Authorization Test Cases
## Prerequisites

- Ranger Admin Server
    - setup ranger admin
    - setup some ranger policies
- Ranger Hive Plugin
    - setup ranger hive plugin for spark
    - configure ranger plugin configurations to connect ranger admin
- Hive Metastore
    - setup Hive Metastore server
    - setup ranger admin and spark connected rightly
- Spark
    - maybe hdfs
    - maybe on yarn
    - spark-shell
    
```bash
bin/spark-shell --proxy-user hzyaoqin --conf spark.sql.warehouse.dir=/user/hzyaoqin/warehouse
```

```bash
hadoop@hzadg-hadoop-dev2:~/spark-2.1.2-bin-hadoop2.7$ bin/spark-shell --proxy-user hzyaoqin --conf spark.sql.warehouse.dir=/user/hzyaoqin/warehouse
18/06/07 11:00:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/06/07 11:00:56 WARN SparkContext: Support for Java 7 is deprecated as of Spark 2.0.0
18/06/07 11:00:59 WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
18/06/07 11:00:59 WARN HiveConf: HiveConf of name hive.exec.partition.num.limit does not exist
18/06/07 11:01:00 WARN HiveConf: HiveConf of name hive.exec.partition.num.limit does not exist
18/06/07 11:01:00 WARN HiveConf: HiveConf of name hive.exec.partition.num.limit does not exist
18/06/07 11:01:00 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark context Web UI available at http://10.201.168.144:4040
Spark context available as 'sc' (master = yarn, app id = application_1522381253211_0116).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.2
      /_/

Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_67)
Type in expressions to have them evaluated.
Type :help for more information.

scala> sc.setLogLevel("info"); import org.apache.spark.sql.catalyst.optimizer.Authorizer; spark.experimental.extraOptimizations ++= Seq(Authorizer)
18/06/07 11:10:50 INFO SharedState: Warehouse path is '/user/hzyaoqin/warehouse'.
18/06/07 11:10:50 INFO HiveUtils: Initializing HiveMetastoreConnection version 1.2.1 using Spark classes.
18/06/07 11:10:51 WARN HiveConf: HiveConf of name hive.exec.partition.num.limit does not exist
18/06/07 11:10:51 INFO metastore: Trying to connect to metastore with URI thrift://hzadg-hadoop-dev2.server.163.org:9083
18/06/07 11:10:51 INFO metastore: Connected to metastore.
18/06/07 11:10:51 INFO SessionState: Created local directory: /tmp/d0216e65-2506-4d7c-8a7d-13f19fba55c0_resources
18/06/07 11:10:51 INFO SessionState: Created HDFS directory: /tmp/hive/hzyaoqin/d0216e65-2506-4d7c-8a7d-13f19fba55c0
18/06/07 11:10:51 INFO SessionState: Created local directory: /tmp/hadoop/d0216e65-2506-4d7c-8a7d-13f19fba55c0
18/06/07 11:10:51 INFO SessionState: Created HDFS directory: /tmp/hive/hzyaoqin/d0216e65-2506-4d7c-8a7d-13f19fba55c0/_tmp_space.db
18/06/07 11:10:51 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is /user/hzyaoqin/warehouse
import org.apache.spark.sql.catalyst.optimizer.Authorizer

scala>
```
## RunnableCommand

### AlterDatabasePropertiesCommand

```sql
ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
```

```sql
sql("alter database default set dbproperties ('abc'='xyz')").show
```

### AlterTableAddColumnsCommand

```sql
ALTER TABLE table_identifier ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
```

### AlterTableChangeColumnCommand

```sql
ALTER TABLE table_identifier
    CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
    [FIRST | AFTER column_name];
```

### AlterTableDropPartitionCommand

```sql
ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
```

```sql

```

### AlterTableRecoverPartitionsCommand

```sql
ALTER TABLE table RECOVER PARTITIONS;
MSCK REPAIR TABLE table;
```

### AlterTableRenamePartitionCommand

```sql
ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
```


### AlterTableRenameCommand

```sql
ALTER TABLE table1 RENAME TO table2;
ALTER VIEW view1 RENAME TO view2;

```

```sql
sql("alter table src10 rename to src11").show
```

### AlterTableSetPropertiesCommand

```sql
ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
ALTER VIEW view1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
```

### AlterTableUnsetPropertiesCommand

```sql
ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
ALTER VIEW view1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
```

### AlterTableSerDePropertiesCommand

```sql
ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
```

### AlterTableSetLocationCommand
```sql
ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "loc";
```

### AlterViewAsCommand

```sql
ALTER VIEW test_view AS SELECT 3 AS i, 4 AS j
```

Treat select clause as QUERY Hive Operation

### AnalyzeColumnCommand

```sql
ANALYZE TABLE tableName COMPUTE STATISTICS FOR COLUMNS some_random_column
```

### AnalyzeTableCommand

```sql
analyze table tableName compute statistics noscan;
```

### AnalyzePartitionCommand

```sql
ANALYZE TABLE t PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS;
ANALYZE TABLE t PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS;
ANALYZE TABLE t PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS;
ANALYZE TABLE t PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS noscan;
ANALYZE TABLE t PARTITION(ds, hr=11) COMPUTE STATISTICS noscan;
ANALYZE TABLE t PARTITION(ds, hr) COMPUTE STATISTICS;
```

### CreateDatabaseCommand

```sql
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
    [COMMENT database_comment]
    [LOCATION database_directory]
    [WITH DBPROPERTIES (property_name=property_value, ...)];
```

### CreateDataSourceTableAsSelectCommand

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
    USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
    AS SELECT ...
```

### CreateHiveTableAsSelectCommand

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
    STORED AS format AS SELECT ...
```

### CreateFunctionCommand

```sql
CREATE TEMPORARY FUNCTION functionName AS className [USING JAR|FILE 'uri' [, JAR|FILE 'uri']]

CREATE FUNCTION [databaseName.]functionName AS className [USING JAR|FILE 'uri' [, JAR|FILE 'uri']]
```

### CreateTableCommand

```sql

CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
   [(col1 data_type [COMMENT col_comment], ...)]
   [COMMENT table_comment]
   [PARTITIONED BY (col3 data_type [COMMENT col_comment], ...)]
   [CLUSTERED BY (col1, ...) [SORTED BY (col1 [ASC|DESC], ...)] INTO num_buckets BUCKETS]
   [SKEWED BY (col1, col2, ...) ON ((col_value, col_value, ...), ...)
   [STORED AS DIRECTORIES]
   [ROW FORMAT row_format]
   [STORED AS file_format | STORED BY storage_handler_class [WITH SERDEPROPERTIES (...)]]
   [LOCATION path]
   [TBLPROPERTIES (property_name=property_value, ...)]
   [AS select_statement];
 
```

### CreateDataSourceTableCommand

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
   [(col1 data_type [COMMENT col_comment], ...)]
   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
```

### CreateTableLikeCommand

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
   LIKE [other_db_name.]existing_table_name
```

### CreateViewCommand

```sql
CREATE view t1 partitioned 
ON (ds) AS 
SELECT * 
FROM   ( 
              SELECT KEY, 
                     value, 
                     ds 
              FROM   t1_new 
              UNION ALL 
              SELECT KEY, 
                     value, 
                     t1_old.ds 
              FROM   t1_old 
              JOIN   t1_mapping 
              ON     t1_old.keymap = t1_mapping.keymap 
              AND    t1_old.ds = t1_mapping.ds ) subq;
```

### CacheTableCommand

```sql
CACHE TABLE testCacheTable AS SELECT * FROM src;
```

### CreateTempViewUsing

### DescribeColumnCommand

```sql 
 DESCRIBE [EXTENDED|FORMATTED] table_name column_name;
```

### DescribeDatabaseCommand

```sql
DESCRIBE DATABASE [EXTENDED] db_name
```

### DescribeFunctionCommand

```sql
DESCRIBE FUNCTION [EXTENDED] upper;
```

### DescribeTableCommand

```sql
DESCRIBE [EXTENDED|FORMATTED] table_name partitionSpec?;
```

### DropDatabaseCommand

```sql
DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
```


### DropFunctionCommand

```sql
DROP TEMPORARY FUNCTION helloworld;
DROP TEMPORARY FUNCTION IF EXISTS helloworld;
DROP FUNCTION hello.world;
DROP FUNCTION IF EXISTS hello.world;
```


### DropTableCommand

```sql
DROP TABLE [IF EXISTS] table_name;
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

### ExplainCommand

```sql
EXPLAIN (EXTENDED | CODEGEN) SELECT * FROM ...
```

### InsertIntoDataSourceCommand

### InsertIntoDataSourceDirCommand

```sql
INSERT OVERWRITE DIRECTORY (path=STRING)?
   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
   SELECT ...
```

### InsertIntoHadoopFsRelationCommand


### InsertIntoHiveDirCommand
```sql
INSERT OVERWRITE [LOCAL] DIRECTORY
   path
   [ROW FORMAT row_format]
   [STORED AS file_format]
   SELECT ...
```

### LoadDataCommand
```sql
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
  [PARTITION (partcol1=val1, partcol2=val2 ...)]
```


### SaveIntoDataSourceCommand

### SetCommand
```sql
set key = value;
set -v;
set;
```

### SetDatabaseCommand

```sql
USE database_name;
```

### ShowCreateTableCommand

```sql
SHOW CREATE TABLE t;
```

### ShowColumnsCommand
```sql
SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
```

### ShowDatabasesCommand
```sql
SHOW (DATABASES|SCHEMAS) [LIKE 'identifier_with_wildcards'];
```

### ShowFunctionsCommand
```sql
SHOW FUNCTIONS [LIKE pattern]
```

### ShowPartitionsCommand
```sql
SHOW PARTITIONS [db_name.]table_name [PARTITION(partition_spec)];
```

### ShowTablesCommand
```sql
SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
```

### ShowTablePropertiesCommand
```sql
SHOW TBLPROPERTIES table_name[('propertyKey')];
```


### TruncateTableCommand

```sql
TRUNCATE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
```


### UncacheTableCommand

```sql
uncache table t;
```

### Ignored

```
// AddFileCommand
// AddJarCommand
// ...

```

## Projection

## LeafNode

## UnaryNode

## BinaryNode

## Union

