import java.net.URI

import com.emc.ecs.spark.sql.sources.s3content.{ObjectContentRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.emc.ecs.spark.sql.sources.s3._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer


object Example extends App {
  if(args.length < 4) {
    println("usage: Example [http://endpoint:9020] [userName] [secretKey] [bucket] [optional: sql string]")
    sys.exit()
  }
  
  val endpointUri = new URI(args(0))
  val credential = (args(1), args(2))
  val argbucket = args(3)


  var sqlStr = "select * from %s where `vocab`= 'good' ".format(argbucket).stripMargin
  if (args.length == 5) {
    sqlStr = args(4)
  }

  print("JMC going to create sparkConf")
  val sparkConf = new SparkConf()
    .setAppName("spark-object")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  print("JMC going to read bucket to get the metadata fields")

  val df = sqlContext.read.bucket(endpointUri, credential, argbucket, withSystemMetadata = false, withContent = true)
  //val df = sqlContext.read.bucket(endpointUri, credential, argbucket, withSystemMetadata = false)
  df.registerTempTable(argbucket)

  print("JMC2 created the registered the temp table. Now going to run sql statement\n")
  //sqlContext.sql(sqlStr.format(argbucket).stripMargin)
  val theData = sqlContext.sql(sqlStr)
  theData.rdd.foreach(println)
  println("JMC-------------------------------------------------------------")
  println(sqlContext.sql(sqlStr.format(argbucket).stripMargin).count)
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")






  /////////////////////////////////////////////////////////////////////////
  // doing this, in an attempt at getting all the values from the "Key" column
  // seemed to cause the custom query functions to be called
  //val sz = df.map{row=>row.getAs("Key")}
  //val x = df.select("Key").collect()
  //theData.foreach(f:Row=>fruits+=f)

  //not sure what this will give me yet...
  //var fruits = new ListBuffer[Row]()
  //for(r<-theData)fruits+=_

  //this isn't really right because theData is just an array of arrays
  //there isn't any column name info like an array of dicts in python or
  //possibly the dataframe, which seems to trigger the custom query
  //val sz = theData.map{row=>row.getAs("Key")}
  //println("Here is just the 'Key' column value: " + sz)

  val keyDf = theData.toDF()
  //val sz = keyDf.map{row=>row.getAs[Any]("Key")} //return RDD[R]
  val sz: RDD[Row] = keyDf.rdd
  sz.map({_.getAs[String]("Key")})

  //val s3ClientWalletImpl = new S3ClientWalletImpl("http://10.1.51.83:9020", credential, argbucket)

  val objectContentRDD = new ObjectContentRDD(sz, endpointUri.toString(), credential, argbucket)
  println("JMC There are this many objects retrieved: " + objectContentRDD.count())
  println("JMC------------------------OBJECT CONTENT------------------------------------")
  objectContentRDD.foreach(println)
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^OBJECT CONTENT^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


  val dfSchema = StructType(Seq(StructField("Key", StringType, false),StructField("ObjectContent", StringType, true)))
  /*
  case class ObjectContentCase(key:String, content:String)
  val contentdf = objectContentRDD.map{
    case Row(val1:String, val2:String) => ObjectContentCase(key = val1, content=val2)
  }
  */



  val contentRDDRows = objectContentRDD.map{
    case Row(val1:String, val2:String) => Row(val1, val2)
  }
  val contentDf = sqlContext.createDataFrame(contentRDDRows, dfSchema)
  val joinedDf = contentDf.join(keyDf, "Key")
  println("JMC------------------------JOINED OBJECT CONTENT------------------------------------")
  joinedDf.rdd.foreach(println)
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^JOINED OBJECT CONTENT^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

  println("JMC------------------------getting json content------------------------------------")
  //val jsonUrl = "s3n://" + args(1) + ":" + args(2) + "/" + argbucket + "/*"
  //val jsonData = sc.textFile(jsonUrl)

  val jsonUrl = "s3a://10.1.83.125:9020/" + args(1) + ":" + args(2) + "/" + argbucket + "/*"
  val jsonData = sc.textFile(jsonUrl)
  println("Number of jsonData rows: " + jsonData.count())
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^getting json content^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
}

