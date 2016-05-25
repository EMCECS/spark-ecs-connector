import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.emc.ecs.spark.sql.sources.s3._

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

  //val df = sqlContext.read.bucket(endpointUri, credential, argbucket, withSystemMetadata = false, withObjectContent = true)
  val df = sqlContext.read.bucket(endpointUri, credential, argbucket, withSystemMetadata = false)
  df.registerTempTable(argbucket)

  print("JMC created the registered the temp table. Now going to run sql statement\n")
  //sqlContext.sql(sqlStr.format(argbucket).stripMargin)
  val theData = sqlContext.sql(sqlStr)
  theData.foreach(println)


  println("JMC-------------------------------------------------------------")
  println(sqlContext.sql(sqlStr.format(argbucket).stripMargin).count)
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
}
