package com.guavus.hotfoot.util

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.net.URI
import java.util.Properties

import com.guavus.hotfoot.HotfootException
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.util.Try


/**
 * Created by yash.datta on 10/05/15.
 */
private[hotfoot] object Utils extends Logging {

  /** Return the path of the default Hotfoot properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("HOTFOOT_CONF_DIR")
      .orElse(env.get("HOTFOOT_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}hotfoot-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  /** Return the path of the default Hotfoot schema file. */
  def getDefaultSchemaFile(env: Map[String, String] = sys.env): String = {
    env.get("HOTFOOT_CONF_DIR")
      .orElse(env.get("HOTFOOT_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}hotfoot-schema.json")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new HotfootException(s"Failed when loading Hotfoot properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash, but this is inconsistent with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path

    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }

    Option(uri.getScheme) match {
      case Some(windowsDrive(d)) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case None =>
        // Preserve fragments for HDFS file name substitution (denoted by "#")
        // For instance, in "abc.py#xyz.py", "xyz.py" is the name observed by the application
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case Some(other) =>
        uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /**
   * Get the ClassLoader which loaded Hotfoot.
   */
  def getHotfootClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrHotfootClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getHotfootClassLoader)

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    Try { Class.forName(clazz, false, getContextOrHotfootClassLoader) }.isSuccess
  }

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrHotfootClassLoader)
  }


  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Whether the underlying operating system is Mac OS X.
   */
  val isMac = SystemUtils.IS_OS_MAC_OSX

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")
}
