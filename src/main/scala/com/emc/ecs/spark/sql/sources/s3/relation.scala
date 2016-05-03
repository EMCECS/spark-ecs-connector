package com.emc.ecs.spark.sql.sources.s3

import java.net.URI
import java.sql.Timestamp
import org.joda.time.Instant

import com.emc.`object`.s3.bean._
import com.emc.`object`.s3.request.QueryObjectsRequest
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import QueryMetadataType._

/**
  * Bucket metadata as a Spark SQL relation.
  */
private class BucketMetadataRelation(
    override val credential: (String,String),
    override val endpointUri: URI,
    val bucketName: String,
    val withSystemMetadata: Boolean)
    (@transient override val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with HasClient with Logging  {

  import Conversions._

  lazy val sysKeys = s3Client.listSystemMetadataSearchKeys()
  lazy val metadataKeys = s3Client.listBucketMetadataSearchKeys(bucketName)

  override def schema: StructType = {
    val sysmd = new MetadataBuilder().putString(SqlMetadataKeys.MetadataType, SYSMD.toString)
    val usermd = new MetadataBuilder().putString(SqlMetadataKeys.MetadataType, USERMD.toString)

    StructType(
      Option(metadataKeys.getIndexableKeys).map(_.map(_.toStructField(usermd)).toList).getOrElse(Nil) ++
      (if(withSystemMetadata)
        Option(sysKeys.getIndexableKeys).map(_.filterNot(_.getName == "ObjectName").map(_.toStructField(sysmd))).getOrElse(Nil) ++
        Option(sysKeys.getOptionalAttributes).map(_.map(_.toStructField(sysmd))).getOrElse(Nil)
      else Nil) ++
      Seq(StructField("Key", StringType, nullable = false, sysmd.putString(SqlMetadataKeys.MetadataName, "ObjectName").build())) ++
      Nil
    )
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    val col2meta = schema.fields.map(f => f.name -> f.metadata.getString(SqlMetadataKeys.MetadataName)).toMap

    val expression = QueryGenerator.toExpression(filters, col2meta) match {
      case "" => throw new UnsupportedOperationException("Unsupported metadata search query.")
      case s => s
    }

    new BucketMetadataRDD(sc, schema, credential, endpointUri, bucketName, requiredColumns, expression)
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
    val query: String)
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
      .map(_.metadata.getString(SqlMetadataKeys.MetadataName))

    val request = new QueryObjectsRequest(bucketName)
      .withQuery(query)
      .withAttributes(projectedAttributes)

    paged(request).flatMap(result => result.getObjects.map(convertToRow))
  }

  case class ObjectData(key: String, usermd: Map[String,String], sysmd: Map[String,String])

  /**
    * Convert the given object metadata to a Spark SQL row object.
    */
  private def convertToRow(obj: QueryObject): Row = {
    val data = ObjectData(
      obj.getObjectName,
      obj.getQueryMds.find(_.getType == USERMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty),
      obj.getQueryMds.find(_.getType == SYSMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty))

    Row.fromSeq(cols.map(_.apply(data)))
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

    new BucketMetadataRelation((identity, secretKey), endpointUri, bucketName, withSystemMetadata)(sqlContext)
  }
}

object DefaultSource {
  val BucketName = "bucket"
  val WithSystemMetadata = "sysmd"
  val Endpoint = "endpoint"
  val Identity = "identity"
  val SecretKey = "secretKey"
}

