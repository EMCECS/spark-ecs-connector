
package com.emc.ecs.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, Suite}

private[sql] abstract trait SharedSQLContext extends SharedSparkContext { self: Suite =>

  @transient private var _sqlContext: SQLContext = _

  def sqlContext: SQLContext = _sqlContext

  override def beforeAll() {
    super.beforeAll()
    _sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    _sqlContext = null
    super.afterAll()
  }
}

trait SharedSparkContext extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false")

  override def beforeAll() {
    _sc = new SparkContext(conf)
    _sc.setLogLevel(org.apache.log4j.Level.WARN.toString)

    super.beforeAll()
  }

  override def afterAll() {
    try {
      _sc.stop()
      _sc = null
    } finally {
      super.afterAll()
    }
  }
}

trait SparkContextProvider {
  def sc: SparkContext
  def conf: SparkConf
}