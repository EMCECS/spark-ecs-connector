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
  import MetadataType._

  lazy val sysKeys = s3Client.listSystemMetadataSearchKeys()
  lazy val metadataKeys = s3Client.listBucketMetadataSearchKeys(bucketName)

  override def schema: StructType = {
    val sysmd = new MetadataBuilder().putString(SqlMetadataKeys.MetadataType, SYSMD.toString).build()
    val usermd = new MetadataBuilder().putString(SqlMetadataKeys.MetadataType, USERMD.toString).build()

    StructType(
      Option(metadataKeys.getIndexableKeys).map(_.map(_.toStructField(usermd)).toList).getOrElse(Nil) ++
      (if(withSystemMetadata)
        Option(sysKeys.getIndexableKeys).map(_.map(_.toStructField(sysmd))).getOrElse(Nil) ++
        Option(sysKeys.getOptionalAttributes).map(_.map(_.toStructField(sysmd))).getOrElse(Nil)
      else Nil) ++
      Seq(StructField("Key", StringType, nullable = false, sysmd)) ++
      Nil
    )
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    val expression = QueryGenerator.toExpression(filters) match {
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

    log.debug(s"computing partition [${split.index}] with query [${query}]")

    def paged(request: QueryObjectsRequest): Iterator[Seq[BucketQueryObject]] = {
      new Iterator[Seq[BucketQueryObject]] {
        var current: Option[QueryObjectsResult] = None
        override def hasNext: Boolean = current.map(_.isTruncated).getOrElse(true)
        override def next(): Seq[BucketQueryObject] = {
          current match {
            case None => current = Some(s3Client.queryObjects(request))
            case Some(_) => current = Some(s3Client.queryMoreObjects(current.get))
          }
          current.get.getObjects
        }
      }
    }

    val request = new QueryObjectsRequest(bucketName)
      .withQuery(query)
      .withAttribute("LastModified") // hack: hardcoded attribute for demonstration purposes

    paged(request).flatMap(result => result.map(convertToRow))
  }

  case class ObjectData(key: String, usermd: Map[String,String], sysmd: Map[String,String])

  /**
    * Convert the given object metadata to a Spark SQL row object.
    */
  private def convertToRow(obj: BucketQueryObject): Row = {
    val data = ObjectData(
      obj.getObjectName,
      obj.getQueryMds.find(_.getType == MetadataType.USERMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty),
      obj.getQueryMds.find(_.getType == MetadataType.SYSMD).map(_.getMdMap.toMap).getOrElse[Map[String,String]](Map.empty))

    Row.fromSeq(cols.map(_.apply(data)))
  }

  /**
    * An array of column value generators, ordered according to `requiredColumns`.
    */
  private lazy val cols: Array[ObjectData => Any] = {
    requiredColumns.map(schema(_)).map { field =>
      val mdtype = MetadataType.valueOf(field.metadata.getString(SqlMetadataKeys.MetadataType))

      (field.name, mdtype, field.dataType) match {
        case ("Key", MetadataType.SYSMD, StringType) => (o: ObjectData) => o.key
        case ("LastModified", MetadataType.SYSMD, TimestampType) => (o: ObjectData) => o.sysmd.get("mtime").map(_.toLong).map(new Timestamp(_)).orElse(null)
        case (name, MetadataType.SYSMD, _) => (o: ObjectData) => null
        case (name, MetadataType.USERMD, StringType) => (o: ObjectData) => o.usermd.getOrElse(name, null)
        case (name, MetadataType.USERMD, IntegerType) => (o: ObjectData) => o.usermd.get(name).map(_.toInt).orElse(null)
        case (name, MetadataType.USERMD, TimestampType) => (o: ObjectData) => o.usermd.get(name).map(Instant.parse).orElse(null)
        case (name, MetadataType.USERMD, DoubleType) => (o: ObjectData) => o.usermd.get(name).map(_.toDouble).orElse(null)
        case (_, MetadataType.USERMD, dt) => sys.error(s"unsupported dataType [$dt]")
      }
    }
  }
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

