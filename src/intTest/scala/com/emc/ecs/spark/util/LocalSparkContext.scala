package com.emc.ecs.spark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * A local Spark context.
  */
class LocalSparkContext {
  private val master = "local[2]"
  private val appName = "test"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(master)
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  def sqlContext: SQLContext = spark.sqlContext
  def sc: SparkContext = spark.sparkContext

  def close(): Unit = {
    spark.stop()

    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkContext {
  /**
    * Executes a function in a local Spark context.
    */
  def sparkContext(body: LocalSparkContext => Unit): Unit = {
    val c = new LocalSparkContext()
    try {
      body(c)
    }
    finally {
      c.close()
    }
  }
}