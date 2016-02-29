package org.apache.spark.sql.columnar.hotfoot

/**
 * Created by yash.datta on 12/07/15.
 */
import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.expressions.MutableRow

private[hotfoot] trait NullableColumnGenerator extends ColumnGenerator {
  private var nullsBuffer: ByteBuffer = _
  private var nullCount: Int = _
  private var seenNulls: Int = 0

  private var nextNullIndex: Int = _
  private var pos: Int = 0

  abstract override def initialize(size: Int, columnName: String = ""): Unit = {
    nullCount = 0
    nextNullIndex = if (nullCount > 0) nullsBuffer.getInt() else -1
    pos = 0
    super.initialize(size, columnName)
  }

  abstract override def generateTo(row: MutableRow, ordinal: Int): Unit = {
    if (pos == nextNullIndex) {
      seenNulls += 1

      if (seenNulls < nullCount) {
        nextNullIndex = nullsBuffer.getInt()
      }

      row.setNullAt(ordinal)
    } else {
      super.generateTo(row, ordinal)
    }

    pos += 1
  }

  abstract override def hasNext: Boolean = seenNulls < nullCount || super.hasNext
}
