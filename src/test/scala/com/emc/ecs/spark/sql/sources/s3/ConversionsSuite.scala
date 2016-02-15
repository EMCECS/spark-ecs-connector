package com.emc.ecs.spark.sql.sources.s3

import java.sql.Timestamp

import org.apache.spark.sql.sources._
import org.scalatest.{FunSuite, Matchers}

class QueryGeneratorSuite extends FunSuite with Matchers {

  test("expression-and") {
    val expr = QueryGenerator.toExpression(Array(
      EqualTo("x-amz-meta-x", "value"), EqualTo("x-amz-meta-y", "value")))

    expr shouldEqual "x-amz-meta-x=='value' AND x-amz-meta-y=='valu`'"
  }

  test("expression-datatypes") {
    val epoch = new Timestamp(0)
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x=='value'"
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", 1))) shouldEqual "x-amz-meta-x==1"
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", epoch))) shouldEqual "x-amz-meta-x==1970-01-01T00:00:00Z"
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", 1.0))) shouldEqual "x-amz-meta-x==1.0"
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", false))) shouldEqual "x-amz-meta-x=='false'"
  }

  test("expression-operators") {
    QueryGenerator.toExpression(Array(EqualTo("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x=='value'"
    QueryGenerator.toExpression(Array(GreaterThan("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x>'value'"
    QueryGenerator.toExpression(Array(GreaterThanOrEqual("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x>='value'"
    QueryGenerator.toExpression(Array(LessThan("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x<'value'"
    QueryGenerator.toExpression(Array(LessThanOrEqual("x-amz-meta-x", "value"))) shouldEqual "x-amz-meta-x<='value'"
  }
}
