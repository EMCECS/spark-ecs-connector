package com.emc.ecs.spark.sql.sources.s3

import java.net.URI
import java.sql.Timestamp

import org.joda.time.Instant
import com.emc.`object`.s3.{S3Client, S3Config}
import com.emc.`object`.s3.bean.{MetadataSearchDatatype, MetadataSearchKey}
import com.emc.`object`.s3.jersey.S3JerseyClient
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.util.matching.Regex._

/**
  * Conversions for S3 model types.
  */
private[spark] object Conversions {

  private val userMetadataPrefix = """x-amz-meta-(.*)""".r

  implicit class MetadataSearchKeyConversions(key: MetadataSearchKey) {
    def toStructField(metadata: MetadataBuilder, indexable: Boolean): StructField = {
      import SqlMetadataKeys._

      StructField(
        name = key.getName match {
          case userMetadataPrefix(name) => name
          case name: String => name
        },
        dataType = key.getDatatype match {
          case MetadataSearchDatatype.string => StringType
          case MetadataSearchDatatype.integer => IntegerType
          case MetadataSearchDatatype.datetime => TimestampType
          case MetadataSearchDatatype.decimal => DoubleType
          case _ => sys.error(s"unsupported datatype: ${key.getDatatype}")
        },
        nullable = true,
        metadata.putString(MetadataName, key.getName).putBoolean(Indexable, indexable).build()
      )
    }
  }
}

private[spark] object SqlMetadataKeys {
  val MetadataType = "x-ecs-md-type"
  val MetadataName = "x-ecs-md-name"
  val Indexable = "x-ecs-md-indexable"
}

private[spark] object WellKnownSysmd {
  val ObjectName = "ObjectName"
  val ObjectContent = "ObjectContent"
}

private[spark] object QueryGenerator {

  /**
    * Generates a S3 object query expression for the given set of Spark SQL filter predicates.
    * See [[org.apache.spark.sql.sources.PrunedFilteredScan]] for detailed explanation.
    *
    * @param filters the set of conjunctive filters ("and")
    * @param names a map of Spark SQL field names to object metadata names (e.g. 'image-width' -> 'x-amz-meta-image-width')
    * @return an S3-compatible query expression
    */
  def toExpression(filters: Array[Filter], names: Map[String,String]): String = {

    val sb = new StringBuilder

    filters.map { filter => filter match {
      case EqualTo(attribute, value) if names.contains(attribute) => Some(Condition(names(attribute), EQ, value))
      case EqualNullSafe(attribute, value) if names.contains(attribute) => None
      case GreaterThan(attribute, value) if names.contains(attribute) => Some(Condition(names(attribute), GT, value))
      case GreaterThanOrEqual(attribute, value) if names.contains(attribute) => Some(Condition(names(attribute), GTE, value))
      case LessThan(attribute, value) if names.contains(attribute) => Some(Condition(names(attribute), LT, value))
      case LessThanOrEqual(attribute, value) if names.contains(attribute) => Some(Condition(names(attribute), LTE, value))
      case In(attribute, values) if names.contains(attribute) => None
      case IsNull(attribute) if names.contains(attribute) => None
      case IsNotNull(attribute) if names.contains(attribute) => None
      case And(left, right) => None
      case Or(left, right) => None
      case Not(child) => None
      case StringStartsWith(attribute, value) if names.contains(attribute) => None
      case StringEndsWith(attribute, value) if names.contains(attribute) => None
      case StringContains(attribute, value) if names.contains(attribute) => None
      case _ => None }
    }.foreach {
      case Some(condition) if sb.length == 0 => sb ++= condition.toString
      case Some(condition) => sb ++= " and " ++= condition.toString
      case _ =>
    }

    sb.toString()
  }

  case class Condition(selector: String, operator: Operator, argument: Any) {
    override def toString = s"${selector}${operator}${format(argument)}"

    def format(argument: Any): String = argument match {
      case s:String => s"'$s'"
      case i:Number => i.toString
      case t:Timestamp => new Instant(t.getTime).toString
      case a:Any => s"'$a'"
    }
  }

  sealed trait Operator

  object EQ extends Operator { override def toString = "==" }
  object LT extends Operator { override def toString = "<" }
  object GT extends Operator { override def toString = ">" }
  object GTE extends Operator { override def toString = ">=" }
  object LTE extends Operator { override def toString = "<=" }
}

trait HasClient {
  def endpointUri: URI
  def credential: (String, String)

  protected[this] lazy val s3Config = new S3Config(endpointUri)
    .withUseVHost(false)
    .withIdentity(credential._1).withSecretKey(credential._2)

  protected[this] lazy val s3Client = new S3JerseyClient(s3Config)
}

trait Logging {
  
}