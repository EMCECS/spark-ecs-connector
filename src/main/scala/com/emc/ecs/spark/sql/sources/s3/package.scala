package com.emc.ecs.spark.sql.sources

import java.net.URI

import org.apache.spark.sql.{DataFrame, DataFrameReader}

import com.emc.ecs.spark.sql.sources.s3.DefaultSource._

package object s3 {

  /**
    * Extends [[org.apache.spark.sql.DataFrameReader]] to read ECS bucket metadata.
 *
    * @param read the associated reader.
    */
  implicit class BucketMetadataDataFrameReader(read: DataFrameReader) {
    /**
      *
      * Read bucket metadata.
      */
    def bucket(endpointURI: URI, credential: (String, String), bucketName: String, withSystemMetadata: Boolean = false,
               withContent: Boolean = false): DataFrame = {
      val parameters = Seq(
        Endpoint -> endpointURI.toString,
        Identity -> credential._1,
        SecretKey -> credential._2,
        BucketName -> bucketName,
        WithSystemMetadata -> withSystemMetadata.toString,
        WithContent -> withContent.toString).toMap

      read.format(classOf[DefaultSource].getName).options(parameters).load()
    }
  }
}