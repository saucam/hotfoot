package com.guavus.hotfoot.schema

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.json4s.JsonAST.{JArray, JBool, JDouble, JInt, JObject, JString}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by yash.datta on 16/05/15.
 */
object MetadataHotfoot {

  /** Creates a Metadata instance from JSON. */
  def fromJson(json: String): Metadata = {
    fromJObject(parse(json).asInstanceOf[JObject])
  }

  /** Creates a Metadata instance from JSON AST. */
  private[schema] def fromJObject(jObj: JObject): Metadata = {
    val builder = new MetadataBuilder
    jObj.obj.foreach {
      case (key, JInt(value)) =>
        builder.putLong(key, value.toLong)
      case (key, JDouble(value)) =>
        builder.putDouble(key, value)
      case (key, JBool(value)) =>
        builder.putBoolean(key, value)
      case (key, JString(value)) =>
        builder.putString(key, value)
      case (key, o: JObject) =>
        builder.putMetadata(key, fromJObject(o))
      case (key, JArray(value)) =>
        if (value.isEmpty) {
          // If it is an empty array, we cannot infer its element type. We put an empty Array[Long].
          builder.putLongArray(key, Array.empty)
        } else {
          value.head match {
            case _: JInt =>
              builder.putLongArray(key, value.asInstanceOf[List[JInt]].map(_.num.toLong).toArray)
            case _: JDouble =>
              builder.putDoubleArray(key, value.asInstanceOf[List[JDouble]].map(_.num).toArray)
            case _: JBool =>
              builder.putBooleanArray(key, value.asInstanceOf[List[JBool]].map(_.value).toArray)
            case _: JString =>
              builder.putStringArray(key, value.asInstanceOf[List[JString]].map(_.s).toArray)
            case _: JObject =>
              builder.putMetadataArray(
                key, value.asInstanceOf[List[JObject]].map(fromJObject).toArray)
            case other =>
              throw new RuntimeException(s"Do not support array of type ${other.getClass}.")
          }
        }
      case other =>
        throw new RuntimeException(s"Do not support type ${other.getClass}.")
    }
    builder.build()
  }

}
