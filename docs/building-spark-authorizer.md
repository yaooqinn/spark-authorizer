# Building Spark Authorizer 

Spark Authorizer is built using [Apache Maven](http://maven.apache.org). To build it, run:

```bash
git clone https://github.com/yaooqinn/spark-authorizer.git
cd spark-authorizer
// choose a branch of your spark version
git checkout spark-<spark.branch.version>
mvn package
```

## Apache Maven

Notes from Spark: 
> The Maven-based build is the build of reference for Apache Spark.
Building Spark using Maven requires Maven 3.3.9 or newer and Java 7+.
Note that support for Java 7 is deprecated as of Spark 2.0.0 and may be removed in Spark 2.2.0.

So, I suggest you build this library using same Maven / Java / Scala.

## Specifying Spark Authorization for Apache Spark

|Branch| Spark Version| Notes|
|:---:|:---:|:---:|
|master|master|periodically update to catch up|
|spark-2.2|2.2.1| - |
|spark-2.1|2.1.2| - |
