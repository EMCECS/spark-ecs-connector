package com.emc.ecs.spark.util

import java.sql.Timestamp
import java.util.UUID

import com.emc.`object`.s3.bean._
import com.emc.`object`.s3.request.{CreateBucketRequest, DeleteObjectsRequest, PutObjectRequest}
import com.emc.`object`.s3.{S3Client, S3ObjectMetadata}

import scala.collection.JavaConversions._

/**
  * Provides example data for integration testing.
  */
object ExampleData {

  case class Example(datetime1: Timestamp, decimal1: Double, integer1: Integer, string1: String)

  /**
    * The metadata search keys to associate with the bucket.
    */
  val metadataSearchKeys = List(
    new MetadataSearchKey("x-amz-meta-datetime1", MetadataSearchDatatype.datetime),
    new MetadataSearchKey("x-amz-meta-decimal1", MetadataSearchDatatype.decimal),
    new MetadataSearchKey("x-amz-meta-integer1", MetadataSearchDatatype.integer),
    new MetadataSearchKey("x-amz-meta-string1", MetadataSearchDatatype.string)
  )

  /**
    * Creates a map of object metadata.
    */
  def metadata(example: Example) = {
    Map(
      "datetime1" -> example.datetime1.toInstant.toString,
      "decimal1" -> example.decimal1.toString,
      "integer1" -> example.integer1.toString,
      "string1" -> example.string1
    )
  }

  /**
    * Concrete example data.
    */
  val exampleText = "Example Content".getBytes
  val exampleData = List(
    ("obj1", Example(t"2007-01-01T00:00:00.00Z", 1.0, 1, "one"), None),
    ("obj2", Example(t"2007-01-02T00:00:00.00Z", 2.0, 2, "two"), Some(exampleText))
  )

  /**
    * A context providing a bucket pre-populated with searchable object metadata.
    */
  class ExampleDataContext(val bucketName: String)(implicit client: S3Client) {

    private def createBucket(): Unit = {
      val bucketRequest = new CreateBucketRequest(bucketName).withMetadataSearchKeys(metadataSearchKeys)
      client.createBucket(bucketRequest)
    }

    private def put(key: String, metadata: Map[String,String], content: Option[Array[Byte]] = None): Unit = {
      val objMetadata = new S3ObjectMetadata()
      objMetadata.setUserMetadata(metadata)
      val putRequest = new PutObjectRequest(bucketName, key, content.orNull).withObjectMetadata(objMetadata)
      client.putObject(putRequest)
    }

    private def populateSampleData(): Unit = {
      for(obj <- exampleData) {
        put(obj._1, metadata(obj._2), obj._3)
      }
    }

    def close(): Unit = {
      val delRequest = new DeleteObjectsRequest(bucketName).withKeys(exampleData.map(_._1).toArray:_*)
      client.deleteObjects(delRequest)
      client.deleteBucket(bucketName)
    }

    createBucket()
    populateSampleData()
  }

  object ExampleDataContext {
    def apply(implicit client: S3Client): ExampleDataContext = {
      val bucket = s"test-${UUID.randomUUID}"
      new ExampleDataContext(bucket)
    }
  }
}