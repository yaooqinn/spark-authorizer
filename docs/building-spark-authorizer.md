# Building Spark Authorizer 

Spark Authorizer is built using [Apache Maven](http://maven.apache.org). To build it, run:

```bash
mvn clean package
```

Notes from Spark: 
> The Maven-based build is the build of reference for Apache Spark.
Building Spark using Maven requires Maven 3.3.9 or newer and Java 7+.
Note that support for Java 7 is deprecated as of Spark 2.0.0 and may be removed in Spark 2.2.0.

So, I suggest you build this library using same Maven / Java / Scala.

## Building agaist different version of Apache Spark

By default, spark authorizer is build agaist spark 2.1.x, which may be incampitable with other spark main branches.

```bash
# build for spark 2.2.x
maven clean package -Pspark-2.2
```

```bash
# build for spark 2.3.x
maven clean package -Pspark-2.3
```
