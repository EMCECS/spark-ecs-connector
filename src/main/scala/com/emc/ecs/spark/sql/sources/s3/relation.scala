package com.emc.ecs.spark.sql.sources.s3

import java.net.URI
import java.sql.Timestamp
import java.util.concurrent.Executors
import org.joda.time.Instant

import com.emc.`object`.s3.bean._
import com.emc.`object`.s3.request.{GetObjectRequest, QueryObjectsRequest}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import QueryMetadataType._
import WellKnownSysmd._
import SqlMetadataKeys._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Bucket metadata as a Spark SQL relation.
  */
private class BucketMetadataRelation(
    override val credential: (String,String),
    override val endpointUri: URI,
    val bucketName: String,
    val withSystemMetadata: Boolean,
    val withContent: Boolean)
    (@transient override val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with HasClient with Logging  {

  import Conversions._

  lazy val sysKeys = s3Client.listSystemMetadataSearchKeys()
  lazy val metadataKeys = s3Client.listBucketMetadataSearchKeys(bucketName)

  override def schema: StructType = {
    val sysmd = new MetadataBuilder().putString(MetadataType, SYSMD.toString)
    val usermd = new MetadataBuilder().putString(MetadataType, USERMD.toString)

    def special(name: String, mdName: String, dataType: DataType, nullable: Boolean, indexable: Boolean) = {
      StructField(name, dataType, nullable,
        sysmd.putString(MetadataName, mdName).putBoolean(Indexable, indexable).build())
    }

    StructType(
      // define fields for the user metadata
      metadataKeys.getIndexableKeys.map(_.toStructField(usermd, indexable = true)).toList ++

      // optionally define fields for the sys metadata
      (if(withSystemMetadata) {
          //indexable keys except for ObjectName
          val indexableSysKeys = sysKeys.getOptionalAttributes.filter(sysKeys.getIndexableKeys() contains).filterNot(_.getName == ObjectName)

          sysKeys.getOptionalAttributes.filterNot(sysKeys.getIndexableKeys() contains).map(_.toStructField(sysmd, indexable = false)) ++
            sysKeys.getOptionalAttributes.filter(indexableSysKeys contains).map(_.toStructField(sysmd, indexable = true))
        }
        else Nil) ++

      // define a field for the object key
      Seq(special("Key", ObjectName, StringType, nullable = false, indexable = true)) ++

      // define a field for the content
      (if(withContent) Seq(special("Content", ObjectContent, BinaryType, nullable = true, indexable = false)) else Nil) ++

      Nil
    )
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    // construct a map of field name to metadata name for the expression builder; only the indexable fields may be pushed down.
    val col2meta = schema.fields
      .filter(f => f.metadata.getBoolean(Indexable))
      .map(f => f.name -> f.metadata.getString(SqlMetadataKeys.MetadataName)).toMap

    // build an S3 query expression
    val expression = QueryGenerator.toExpression(filters, col2meta) match {
      case "" => throw new UnsupportedOperationException("Unsupported metadata search query.")
      case s => s
    }

    new BucketMetadataRDD(sc, schema, credential, endpointUri, bucketName, requiredColumns, expression, withContent)
  }
}

private case class BucketMetadataPartition(override val index: Int) extends Partition

private class BucketMetadataRDD(
    sc: SparkContext,
    val schema: StructType,
    override val credential: (String,String),
    override val endpointUri: URI,
    val bucketName: String,
    val requiredColumns: Array[String],
    val query: String,
    val withContent: Boolean)
  extends RDD[Row](sc, Nil) with HasClient with Logging {

  /**
    * Note: the bucket metadata has no meaningful partitions.
    */
  override def getPartitions: Array[Partition] = Array(BucketMetadataPartition(0))

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    log.info(s"computing partition [${split.index}] with query [${query}]")

    def paged(request: QueryObjectsRequest): Iterator[QueryObjectsResult] = {
      new Iterator[QueryObjectsResult] {
        var current: Option[QueryObjectsResult] = None
        override def hasNext: Boolean = current.map(_.isTruncated).getOrElse(true)
        override def next(): QueryObjectsResult = {
          current match {
            case None => current = Some(s3Client.queryObjects(request))
            case Some(_) => current = Some(s3Client.queryMoreObjects(current.get))
          }
          current.get
        }
      }
    }

    // apply projection (obtain requiredColumns only)
    val projectedAttributes: Seq[String] =
      requiredColumns.map(schema(_))
      .filter(_.metadata.getString(SqlMetadataKeys.MetadataType) == SYSMD.toString)
      .filterNot(_.name == "Content")
      .map(_.metadata.getString(SqlMetadataKeys.MetadataName))

    val request = new QueryObjectsRequest(bucketName)
      .withQuery(query)
      .withAttributes(projectedAttributes)

    implicit val executionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))
    paged(request).flatMap(p=> readRows(p))
  }
  case class ObjectData(key: String, usermd: Map[String,String], sysmd: Map[String,String], content: Future[Array[Byte]])


  /**
    * Convert the given query result to Spark Sql rows.
    *
    * @param result a page of query results.
    * @return a sequence of rows
    */
  private def readRows(result: QueryObjectsResult)(implicit execctx: ExecutionContext): Seq[Row] = {
    val objects = result.getObjects.map { obj =>
      ObjectData(
        obj.getObjectName,
        obj.getQueryMds.find(_.getType == USERMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty),
        obj.getQueryMds.find(_.getType == SYSMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty),
        if(requiredColumns.contains("Content")) download(obj.getObjectName) else null)
    }

    objects.map { data => Row.fromSeq(cols.map(_.apply(data))) }
  }

  /***
    * Download the object content.
    */
  private def download(objectName: String)(implicit execctx: ExecutionContext) : Future[Array[Byte]] = {
    Future {
      var gor = new GetObjectRequest(bucketName, objectName)
      val getObjResponse: GetObjectResult[Array[Byte]] = s3Client.getObject(gor,classOf[Array[Byte]])
      getObjResponse.getObject
    }
  }

  /**
    * An array of column value generators, ordered according to `requiredColumns`.
    */
  private lazy val cols: Array[ObjectData => Any] = {

    val col2meta = schema.fields.map(f => f.name -> f.metadata.getString(SqlMetadataKeys.MetadataName)).toMap

    requiredColumns.map(schema(_)).map { field =>
      val mdtype = QueryMetadataType.valueOf(field.metadata.getString(SqlMetadataKeys.MetadataType))

      (field.name, mdtype, field.dataType) match {
        case ("Key", SYSMD, StringType) => (o: ObjectData) => o.key
        case ("Content", SYSMD, BinaryType) => (o: ObjectData) => Await.result(o.content, Duration.Inf)
        case (name, SYSMD, StringType) => (o: ObjectData) => o.sysmd.getOrElse(sysmd2result(name), null)
        case (name, SYSMD, IntegerType) => (o: ObjectData) => o.sysmd.get(sysmd2result(name)).filterNot(_.isEmpty).map(_.toInt).orElse(null)
        case (name, SYSMD, TimestampType) => (o: ObjectData) => o.sysmd.get(sysmd2result(name)).filterNot(_.isEmpty).map(_.toLong).map(new Timestamp(_)).orElse(null)
        case (name, SYSMD, DoubleType) => (o: ObjectData) => o.sysmd.get(sysmd2result(name)).filterNot(_.isEmpty).map(_.toDouble).orElse(null)
        case (name, USERMD, StringType) => (o: ObjectData) => o.usermd.getOrElse(col2meta(name), null)
        case (name, USERMD, IntegerType) => (o: ObjectData) => o.usermd.get(col2meta(name)).filterNot(_.isEmpty).map(_.toInt).orElse(null)
        case (name, USERMD, TimestampType) => (o: ObjectData) => o.usermd.get(col2meta(name)).filterNot(_.isEmpty).map(Instant.parse).orElse(null)
        case (name, USERMD, DoubleType) => (o: ObjectData) => o.usermd.get(col2meta(name)).filterNot(_.isEmpty).map(_.toDouble).orElse(null)
        case (_, _, dt) => sys.error(s"unsupported dataType [$dt]")
      }
    }
  }

  /**
    * A map of system metadata attribute names to the actual keys in the query result object
    */
  private val sysmd2result: Map[String,String] = Seq(
    "LastModified" -> "mtime",
    "CreateTime" -> "createtime",
    "Owner" -> "owner",
    "Size" -> "size",
    "ObjectName" -> "object-name",
    "ContentType" -> "ctype",
    "Expiration" -> "expiration",
    "ContentEncoding" -> "encoding",
    "Expires" -> "Expires",
    "Retention" -> "retention"
  ).toMap.withDefaultValue(null)
}

class DefaultSource extends RelationProvider {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val endpointUri = parameters.get(Endpoint).map(URI.create).getOrElse(sys.error(s"'$Endpoint' must be specified"))
    val identity = parameters.getOrElse(Identity, sys.error(s"'$Identity' must be specified"))
    val secretKey = parameters.getOrElse(SecretKey, sys.error(s"'$SecretKey' must be specified"))

    val bucketName = parameters.getOrElse(BucketName, sys.error(s"'$BucketName' must be specified"))
    val withSystemMetadata = parameters.get(WithSystemMetadata).map(_.toBoolean).getOrElse(false)
    val withContent = parameters.get(WithContent).map(_.toBoolean).getOrElse(false)

    new BucketMetadataRelation((identity, secretKey), endpointUri, bucketName, withSystemMetadata, withContent)(sqlContext)
  }
}

object DefaultSource {
  val BucketName = "bucket"
  val WithSystemMetadata = "sysmd"
  val WithContent = "content"
  val Endpoint = "endpoint"
  val Identity = "identity"
  val SecretKey = "secretKey"
}

