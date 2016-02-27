package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.LogicalRDD

/**
 * Created by yash.datta on 26/02/16.
 */
object DataFrameFactory {
  def getDataFrame(sqlContext: SQLContext,
    catalystRows: RDD[InternalRow],
    attributes: Seq[AttributeReference]): DataFrame = {
    val logicalPlan = LogicalRDD(attributes, catalystRows)(sqlContext)
    DataFrame(sqlContext, logicalPlan)
  }
}
