package com.guavus.hotfoot

import java.nio.ByteBuffer

import com.guavus.hotfoot.schema.SchemaParser

import java.io.{File, PrintStream}

import org.apache.spark._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD

// import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.hotfoot._
import org.apache.spark.sql.types._

// import org.apache.spark.sql.columnar.{ColumnType}

/**
 * @author ${user.name}
 */
object Hotfoot extends Logging {

  private[hotfoot] var exitFn: () => Unit = () => System.exit(1)
  private[hotfoot] var printStream: PrintStream = System.err
  private[hotfoot] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[hotfoot] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      __  __       __   ____            __
     / /_/ /___  _/ /_ / _ /___  ___  _/ /_
    /  _  // _ \ / /_ / /_ / _ \/ _ \ / /_
   /_/ /_/ \___//___//_/   \___/\___//___/  version %s

                        """.format(HOTFOOT_VERSION))
    printStream.println("Type --help for more information.")
    exitFn()
  }
  private[hotfoot] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn()
  }

  def main(args : Array[String]) {
    val appArgs = new HotfootArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }

    val sparkConf = new SparkConf()
      .setAppName("Hotfoot")
      .setMaster("local[2]")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // Read configurations

    // parse schema from json file
    // better option to make it a json RDD
    //val schema = sqlContext.jsonFile(appArgs.schemaFile)
    // schema.printSchema()
    // val schema = new Schema.Parser().parse(appArgs.schemaFile)
    val schema = try (SchemaParser.parseJson(new File(appArgs.schemaFile)))
      catch { case e: Throwable =>
      logInfo(s"Schema in provided schema file: ${appArgs.schemaFile} is not in JSON format! ")
      throw e
    }

    println(" schema = " + schema.prettyJson)

    val attributes = SchemaParser.toAttributes(schema)
    // select output format
    val nextRow = new SpecificMutableRow(attributes.map(_.dataType))

    val columnGenerators = attributes.map { attribute =>
      val columnType = ColumnType(attribute.dataType)
      //val initialBufferSize = columnType.defaultSize * batchSize
      ColumnGenerator(attribute.dataType, 0, attribute.name)
    }.toArray

    // TODO: this must be done for any indexing column marked by user in schema
    val rddIndex: RDD[Int] = sc.parallelize(0 to appArgs.numRecords)

    val rdd: RDD[InternalRow] = rddIndex.mapPartitions{ indexIter =>

      val nextRow = new SpecificMutableRow(attributes.map(_.dataType))

      val rows = indexIter.flatMap { partition =>

        // Generate rows via ColumnGenerators
        new Iterator[InternalRow] {
          private[this] val rowLen = nextRow.numFields
          override def next(): InternalRow = {
            var i = 0
            while (i < rowLen) {
              columnGenerators(i).generateTo(nextRow, i)
              i += 1
            }
            if (attributes.isEmpty) InternalRow.empty else nextRow
          }

          override def hasNext: Boolean = columnGenerators(0).hasNext
        }
      }

      rows
    }

    val logicalPlan = LogicalRDD(attributes, rdd)(sqlContext)
    val df = new DataFrame(sqlContext, logicalPlan)
    df.write.parquet(appArgs.outputPath)

  }

}
