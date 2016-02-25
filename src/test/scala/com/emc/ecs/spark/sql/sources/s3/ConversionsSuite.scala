package com.emc.ecs.spark.sql.sources.s3

import java.sql.Timestamp

import org.apache.spark.sql.sources._
import org.scalatest.{FunSuite, Matchers}

class QueryGeneratorSuite extends FunSuite with Matchers {

  val names = Map("x" -> "x-amz-meta-x", "y" -> "x-amz-meta-y")

  test("expression-and") {
    val expr = QueryGenerator.toExpression(Array(EqualTo("x", "value"), EqualTo("y", "value")), names)

    expr shouldEqual "x-amz-meta-x=='value' and x-amz-meta-y=='value'"
  }

  test("expression-datatypes") {
    val epoch = new Timestamp(0)
    QueryGenerator.toExpression(Array(EqualTo("x", "value")), names) shouldEqual "x-amz-meta-x=='value'"
    QueryGenerator.toExpression(Array(EqualTo("x", 1)), names) shouldEqual "x-amz-meta-x==1"
    QueryGenerator.toExpression(Array(EqualTo("x", epoch)), names) shouldEqual "x-amz-meta-x==1970-01-01T00:00:00.000Z"
    QueryGenerator.toExpression(Array(EqualTo("x", 1.0)), names) shouldEqual "x-amz-meta-x==1.0"
    QueryGenerator.toExpression(Array(EqualTo("x", false)), names) shouldEqual "x-amz-meta-x=='false'"
  }

  test("expression-operators") {
    QueryGenerator.toExpression(Array(EqualTo("x", "value")), names) shouldEqual "x-amz-meta-x=='value'"
    QueryGenerator.toExpression(Array(GreaterThan("x", "value")), names) shouldEqual "x-amz-meta-x>'value'"
    QueryGenerator.toExpression(Array(GreaterThanOrEqual("x", "value")), names) shouldEqual "x-amz-meta-x>='value'"
    QueryGenerator.toExpression(Array(LessThan("x", "value")), names) shouldEqual "x-amz-meta-x<'value'"
    QueryGenerator.toExpression(Array(LessThanOrEqual("x", "value")), names) shouldEqual "x-amz-meta-x<='value'"
  }
}
