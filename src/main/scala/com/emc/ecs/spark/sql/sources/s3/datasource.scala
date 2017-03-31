
package com.emc.ecs.spark.sql.sources.s3

import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
  * The default [[RelationProvider]] for the bucket metadata relation.
  */
class DefaultSource extends RelationProvider
  with DataSourceRegister {

  import DefaultSource._

  /**
    * Short alias for bucket metadata data source.
    */
  override def shortName(): String = "s3"

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
