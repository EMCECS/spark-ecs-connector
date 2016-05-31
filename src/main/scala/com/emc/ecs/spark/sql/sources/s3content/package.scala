package com.emc.ecs.spark.sql.sources

import java.net.URI

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
 * Created by conerj on 5/31/16.
 */
package object s3content {

  def getResultsFromMeta(sqlContext:SQLContext, sqlStr:String, credential: (String,String), endpointUri: URI, bucketName: String): DataFrame = {

    //this stuff comes from the zeppelin frame
    //val bucketName = "conerjbucket_meta1"
    //var sqlStr = "select * from %s where `vocab`= 'good' ".format("conerjbucket_meta1")
    val theData = sqlContext.sql(sqlStr)
    theData.foreach(println)
    val keyDf = theData.toDF()
    val sz = keyDf.map{row=>row.getAs[Any]("Key")} //return RDD[R]
    val endpointStr = endpointUri.toString()

    val s3ClientWalletImpl = new S3ClientWalletImpl(endpointStr, credential, bucketName)

    val objectContentRDD = new ObjectContentRDD(sz, credential, endpointUri, bucketName)

    /*
    println("JMC There are this many objects retrieved: " + objectContentRDD.count())
    println("JMC------------------------OBJECT CONTENT------------------------------------")
    objectContentRDD.foreach(println)
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^OBJECT CONTENT^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    */

    val dfSchema = StructType(Seq(StructField("Key", StringType, false),StructField("ObjectContent", StringType, true)))

    val contentRDDRows = objectContentRDD.map{
      case Row(val1:String, val2:String) => Row(val1, val2)
    }
    val contentDf = sqlContext.createDataFrame(contentRDDRows, dfSchema)
    val joinedDf = contentDf.join(keyDf, "Key")

    /*
    println("JMC------------------------JOINED OBJECT CONTENT------------------------------------")
    joinedDf.foreach(println)
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^JOINED OBJECT CONTENT^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    */

    joinedDf.registerTempTable("everything")


    //joinedDf.collect()
    //sqlContext.sql("select * from everything")
    joinedDf

  }
}
