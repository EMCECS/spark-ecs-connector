import java.net.URI
import java.sql.Timestamp

import com.emc.ecs.spark.sql.sources.s3._
import com.emc.ecs.spark.util.ExampleData._
import com.emc.ecs.spark.util.LocalSparkContext.sparkContext
import com.emc.ecs.spark.util.{LocalSparkContext, SharedDataContext}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RelationSuite extends WordSpec with Matchers
  with SharedDataContext {

  val endpointUri = URI.create(System.getProperty("s3.endpoint"))
  val credential = (System.getProperty("s3.accessKeyId"),System.getProperty("s3.secretAccessKey"))

  "A DataSource" when {
    "using SQL" should {
      "support DDL" in sparkContext { ctx =>
        // all options shown below for completeness; sysmd and content are optional
        ctx.sqlContext.sql(
          s"""
            CREATE TEMPORARY VIEW bucket1
            USING s3
            OPTIONS (
              endpoint "${endpointUri}",
              identity "${credential._1}",
              secretKey "${credential._2}",
              bucket "${dataContext.bucketName}",
              sysmd "true",
              content "false"
            )
          """)
        val df = ctx.sqlContext.table("bucket1")
      }
    }
  }

  "A DataFrameReader" when {
    "using Scala" should {
      "provide an extension method" in sparkContext { ctx =>
        val df = ctx.sqlContext.read.bucket(endpointUri, credential, dataContext.bucketName)
      }
    }
  }

  "A Relation" when {
    def bucket(withSystemMetadata: Boolean = false, withContent: Boolean = false)(implicit ctx: LocalSparkContext) = {
      ctx.sqlContext.read.bucket(endpointUri, credential, dataContext.bucketName, withSystemMetadata, withContent)
    }

    "created" should {
      "contain user schema" in sparkContext { implicit ctx =>
        val df = bucket()
        val fields = df.schema.fields.map(f => s"x-amz-meta-${f.name}")
        fields should contain theSameElementsAs (metadataSearchKeys.map(_.getName) ++ List("x-amz-meta-Key"))
      }

      "contain sys schema" in sparkContext { implicit ctx =>
        val df = bucket(withSystemMetadata = true)
        df.schema.fields.map(_.name) should contain allOf ("LastModified", "Owner", "Size", "ContentType", "Etag")
      }

      "support table registration" in sparkContext { implicit ctx =>
        val df = bucket()
        df.createTempView("bucket1")
        val df2 = ctx.sqlContext.table("bucket1")
      }
    }

    "queried" should {
      "require at least one indexed column" in sparkContext { implicit ctx =>
        intercept[UnsupportedOperationException] {
          val df = bucket()
          df.collect()
        }
      }
      "push-down indexed columns" ignore {
        // TODO verify predicate push-down
      }
    }

    "projected" should {
      "support sys columns" in sparkContext { implicit ctx =>
        val df = bucket(withSystemMetadata = true).where("string1 > ''")
        val rows = df.select("ContentType", "CreateTime").collect()
        rows(0).getAs[String]("ContentType") shouldEqual ("application/octet-stream")
        rows(0).getAs[Timestamp]("CreateTime") should not be (null)
      }

      "support user columns" in sparkContext { implicit ctx =>
        import ctx.spark.implicits._
        val expected = exampleData.head._2.integer1
        val df = bucket().where($"integer1" === expected)
        val rows = df.select("integer1").collect()
        rows(0).getAs[Int]("integer1") shouldEqual expected
      }

      "push-down projection" ignore {
        // TODO verify projection push-down
      }
    }

    "converted to DataSet" should {
      "support case classes" in sparkContext { implicit ctx =>
        import ctx.spark.implicits._
        val df = bucket().where("string1 > ''")
        val ds = df.as[Example]
        val actual = ds.collect()
        actual should contain theSameElementsAs exampleData.map(_._2)
      }
    }

    "queried with content" should {
      "download" in sparkContext { implicit ctx =>
        import ctx.spark.implicits._
        val expected = exampleData.find(_._3.isDefined).head // find an example with non-empty content
        val df = bucket(withContent = true).where($"integer1" === expected._2.integer1)
        val actual = df.collect().head
        actual.getAs[Array[Byte]]("Content") should contain theSameElementsAs expected._3.get
      }
    }
  }
}


