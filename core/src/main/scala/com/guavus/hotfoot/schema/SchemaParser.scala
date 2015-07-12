package com.guavus.hotfoot.schema

import java.io._

import com.guavus.hotfoot.Hotfoot._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.Logging
import org.apache.spark.sql.types._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JArray, JBool, JObject, JString, JValue}

import scala.util.Try

/**
 * Created by yash.datta on 11/05/15.
 */
object SchemaParser extends Logging  {
  /** Parse a schema from the provided file.
    * If named, the schema is added to the names known to this parser. */
  @throws(classOf[IOException])
  def parseJson(file: File): DataType = {
    parseDataType(parse(FileInput(file)))
  }

  def toAttributes(file: File): Seq[Attribute] = {
    val dType = try parseJson(file)
    catch { case e: Throwable =>
      logInfo(s"Schema in provided schema file: $file is not in JSON format! ")
      throw e
    }
    toAttributes(dType)
  }

  def toAttributes(dType: DataType): Seq[Attribute] = {
    dType match {
      case s: StructType => s.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      case other => sys.error(s"Cannot convert $dType to row")
    }
  }

  /** Parse a schema from the provided string.
    * If named, the schema is added to the names known to this parser. */
  def parseJson(s: String): DataType = DataType.fromJson(s)

  private val nonDecimalNameToType = {
    Seq(NullType, DateType, TimestampType, BinaryType,
      IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType)
      .map(t => t.typeName -> t).toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    name match {
      case "decimal" => DecimalType.Unlimited
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType(other)
    }
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      nameToType(name)

    case JSortedObject(
    ("containsNull", JBool(n)),
    ("elementType", t: JValue),
    ("type", JString("array"))) =>
      ArrayType(parseDataType(t), n)

    case JSortedObject(
    ("keyType", k: JValue),
    ("type", JString("map")),
    ("valueContainsNull", JBool(n)),
    ("valueType", v: JValue)) =>
      MapType(parseDataType(k), parseDataType(v), n)

    case JSortedObject(
    ("fields", JArray(fields)),
    ("type", JString("struct"))) =>
      StructType(fields.map(parseStructField))

    case JSortedObject(
    ("class", JString(udtClass)),
    ("pyClass", _),
    ("sqlType", _),
    ("type", JString("udt"))) =>
      Class.forName(udtClass).newInstance().asInstanceOf[UserDefinedType[_]]
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
    ("metadata", metadata: JObject),
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable, MetadataHotfoot.fromJObject(metadata))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
  }
}
