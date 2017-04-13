package com.emc.ecs.spark.util

import com.emc.ecs.spark.sql.sources.s3.HasClient
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Provides example data.
  */
trait SharedDataContext extends BeforeAndAfterAll with HasClient {
  self: Suite =>

  import com.emc.ecs.spark.util.ExampleData._

  @transient protected implicit var dataContext: ExampleDataContext = _

  override def beforeAll(): Unit = {
    dataContext = ExampleDataContext(s3Client)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dataContext.close()
  }
}
