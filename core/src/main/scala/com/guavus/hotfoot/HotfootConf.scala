package com.guavus.hotfoot

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap


private[hotfoot] object HotfootConf {
  val COMPRESS_CACHED = "hotfoot.columnarStorage.compressed"
  val COLUMN_BATCH_SIZE = "hotfoot.batchSize"
  val PARQUET_COMPRESSION = "spark.sql.parquet.compression.codec"
}

/**
 * Created by yash.datta on 15/06/15.
 */
private[hotfoot] class HotfootConf(hotfootProperties: HashMap[String, String])
  extends Serializable {
  import HotfootConf._

  // Load any spark.* system properties
  for ((key, value) <- hotfootProperties if key.startsWith("spark.")) {
    set(key, value)
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): HotfootConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }

    settings.put(key, value)
    this
  }

  @transient private val settings = new ConcurrentHashMap[String, String]()

  /** When true tables cached using the in-memory columnar caching will be compressed. */
  private[hotfoot] def useCompression: Boolean = getConf(COMPRESS_CACHED, "true").toBoolean

  /** The compression codec for writing to a Parquetfile */
  private[hotfoot] def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION, "gzip")

  /** The number of rows that will be  */
  private[hotfoot] def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE, "10000").toInt


  /** ********************** HotfootConf functionality methods ************ */

  /** Set Hotfoot configuration properties. */
  def setConf(props: Properties): Unit = {
    props.foreach { case (k, v) => settings.put(k, v) }
  }

  /** Set the given Hotfoot configuration property. */
  def setConf(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    settings.put(key, value)
  }

  /**
   * Return the value of Hotfoot configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   */
  def getConf(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfs: immutable.Map[String, String] = settings.synchronized { settings.toMap }

  private[hotfoot] def unsetConf(key: String) {
    settings -= key
  }

  private[hotfoot] def clear() {
    settings.clear()
  }
}
