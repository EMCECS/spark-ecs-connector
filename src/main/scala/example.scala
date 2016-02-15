import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.emc.ecs.spark.sql.sources.s3._

object Example extends App {
  val endpointUri = new URI(args(0))
  val credential = (args(1), args(2))

  val sparkConf = new SparkConf().setAppName("spark-object")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.bucket(endpointUri, credential, "ben_bucket", withSystemMetadata = false)
  df.registerTempTable("ben_bucket")

  sqlContext.sql(
    """
      |SELECT * FROM ben_bucket
      |WHERE `x-amz-meta-image-viewcount` >= 5000 AND `x-amz-meta-image-viewcount` <= 5500
    """.stripMargin).show(100)

  println(sqlContext.sql(
    """
      |SELECT * FROM ben_bucket
      |WHERE `x-amz-meta-image-width` > 0
    """.stripMargin).count)

}
