
# Bucket Metadata Search with Spark SQL (2.x)

The [spark-ecs-s3](https://github.com/emcvipr/spark-ecs-s3) project makes it possible to view an ECS bucket as a Spark dataframe. 
Each row in the dataframe corresponds to an object in the bucket, and each column coresponds to a piece of object metadata.

**How it Works**

Spark SQL supports querying external data sources and rendering the results as a dataframe.   With the [PrunedFilteredScan](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.sources.PrunedFilteredScan) trait, the external data source handles column pruning and predicate pushdown.  In other words, the WHERE clause is pushed to ECS by taking advantage of the bucket metadata search feature of ECS 2.2.

![Screenshot](screenshot.png)

# Using

_We're working on publishing the library to Maven Central.  In the meantime, please build the library from sources._

## Building
The project use the Gradle build system and includes a script that automatically downloads Gradle.

Build and install the library to your local Maven repository as follows:
```
$ ./gradlew publishShadowPublicationToMavenLocal
```

### Linking to your Spark 2.x Application
Link to the library using the following dependency: `com.emc.ecs:spark-ecs-s3_2.11:jar:1.4-SNAPSHOT`

## Zeppelin
1. Install Zeppelin 0.7+.
2. `export SPARK_LOCAL_IP=127.0.0.1`
3. `bin/zeppelin.sh`

Create a notebook with the following commands.   Replace `***` with your S3 credentials.

```
%dep
z.load("com.emc.ecs:spark-ecs-s3_2.11:jar:1.4-SNAPSHOT")
```

```
import java.net.URI
import com.emc.ecs.spark.sql.sources.s3._

val endpointUri = new URI("http://10.1.83.51:9020/")
val credential = ("***ACCESS KEY ID***", "***SECRET ACCESS KEY***")

val df = sqlContext.read.bucket(endpointUri, credential, "ben_bucket", withSystemMetadata = false)
df.createOrReplaceTempView("ben_bucket")
```

```
%sql
SELECT * FROM ben_bucket 
WHERE `image-viewcount` >= 5000 AND `image-viewcount` <= 10000
```

## TODO
1. Implement 'OR' pushdown.  ECS supports 'or', but not in combination with 'and'.
2. Avoid sending a query containing a non-indexable key.
