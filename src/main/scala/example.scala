import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.emc.ecs.spark.sql.sources.s3._

object Example extends App {
  if(args.length != 3) {
    println("usage: Example [http://endpoint:9020] [userName] [secretKey]")
    sys.exit()
  }
  val endpointUri = new URI(args(0))
  val credential = (args(1), args(2))

  val sparkConf = new SparkConf()
    .setAppName("spark-object")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.bucket(endpointUri, credential, "ben_bucket", withSystemMetadata = true)
  df.registerTempTable("ben_bucket")

  sqlContext.sql(
    """
      |SELECT * FROM ben_bucket
      |WHERE `image-viewcount` >= 5000 AND `image-viewcount` <= 10000
    """.stripMargin).show(100)

  println(sqlContext.sql(
    """
      |SELECT * FROM ben_bucket
      |WHERE `image-width` > 0
    """.stripMargin).count)

}
