package com.guavus.hotfoot

import com.guavus.hotfoot.launcher.HotfootOptionParser
import com.guavus.hotfoot.util.Utils

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Parses and encapsulates arguments from HotfootGenerate script.
 * The env argument is used for testing.
 */
private[hotfoot] class HotfootArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends HotfootOptionParser {
  var propertiesFile: String = null
  var schemaFile: String = null
  var verbose: Boolean = false
  val hotfootProperties: HashMap[String, String] = new HashMap[String, String]()
  var numRecords = 100
  var outPutFormat: String = null
  var outputPath: String = null
  var jars: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  /** Default properties present in the currently defined defaults file. */
  // scalastyle:off println
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    if (verbose) Hotfoot.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      Utils.getPropertiesFromFile(filename).foreach { case (k, v) =>
        defaultProperties(k) = v
        if (verbose) Hotfoot.printStream.println(s"Adding default property: $k=$v")
      }
    }
    defaultProperties
  }

  // scalastyle:on println
  // Set parameters from command line arguments
  try {
    parse(args.toList)
  } catch {
    case e: IllegalArgumentException =>
      Hotfoot.printErrorAndExit(e.getMessage())
  }
  // Populate `hotfootProperties` map from properties file
  mergeDefaultHotfootProperties()
  // Remove keys that don't start with "hotfoot." from `hotfootProperties`.
  // TODO: Should we do this ?
  // ignoreNonSparkProperties()
  // Use `hotfootProperties` map along with env vars to fill in any missing parameters
  loadEnvironmentArguments()

  validateArguments()

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   */
  private def mergeDefaultHotfootProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    schemaFile = Option(schemaFile).getOrElse(Utils.getDefaultSchemaFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!hotfootProperties.contains(k)) {
        hotfootProperties(k) = v
      }
    }
  }

  /**
   * Remove keys that don't start with "hotfoot." from `hotfootProperties`.
   */
  private def ignoreNonSparkProperties(): Unit = {
    hotfootProperties.foreach { case (k, v) =>
      if (!k.startsWith("hotfoot.")) {
        hotfootProperties -= k
        Hotfoot.printWarning(s"Ignoring non-hotfoot config property: $k=$v")
      }
    }
  }

  /**
   * Load arguments from environment variables, Hotfoot properties etc.
   */
  private def loadEnvironmentArguments(): Unit = {

    jars = Option(jars).orElse(hotfootProperties.get("hotfoot.jars")).orNull


  }

  private def validateArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }

    if (schemaFile == null) {
      Hotfoot.printErrorAndExit("Must specify schema json file resource (JAR or Python or R file)")
    }

    if (numRecords <= 0) {
      Hotfoot.printErrorAndExit("Must specify number of records greater than 0")
    }
  }

  override def toString: String = {
    s"""Parsed arguments:
    |  propertiesFile          $propertiesFile
    |  schemaFile              $schemaFile
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties file $propertiesFile:
    |${hotfootProperties.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case PROPERTIES_FILE =>
        propertiesFile = value

      case SCHEMA_FILE =>
        schemaFile = value

      case RECORDS_NUM =>
        numRecords = Integer.getInteger(value)

      case OUTPUT_FORMAT =>
        outPutFormat = value

      case OUTPUT_PATH =>
        outputPath = value

      case JARS =>
        jars = Utils.resolveURIs(value)

      case CONF =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => hotfootProperties(k) = v
          case _ => Hotfoot.printErrorAndExit(s"Hotfoot config without '=': $value")
        }

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        Hotfoot.printVersionAndExit()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
   * Handle unrecognized command line options.
   *
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
   */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      Hotfoot.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    false
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra
  }

  // scalastyle:off println
  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    val outStream = Hotfoot.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    outStream.println(
      """Usage: hotfoot-generate [options] <app jar | python file> [app arguments]
        |
        |Options:
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/hotfoot-defaults.conf.
        |  --schema-file SFILE         Path to a json file that specifies the schema of data to be
        |                              written.
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output
        |  --version,                  Print the version of current Spark
        |
      """.stripMargin
    )
    // scalastyle:on println
    Hotfoot.exitFn()
  }

}
