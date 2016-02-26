package org.apache.spark.sql.columnar.hotfoot

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData, UnsafeRow}

// import org.apache.spark.sql.columnar.NullableColumnAccessor
// import org.apache.spark.sql.columnar.compression.CompressibleColumnAccessor
import org.apache.spark.sql.columnar.hotfoot._
import org.apache.spark.sql.types._

/**
 * An `Iterator` like trait used to generate values for columns.
 * Instead of directly returning it, the value is set into some field of
 * a [[MutableRow]]. In this way, boxing cost can be avoided by leveraging the setter methods
 * for primitive values provided by [[MutableRow]].
 */

/**
 * Created by yash.datta on 05/07/15.
 */

private[hotfoot] trait ColumnGenerator {

  initialize()

  protected def initialize()

  def hasNext: Boolean

  protected  def initialize(numVals: Int, columnName: String = "")

  def generateTo(row: MutableRow, ordinal: Int)

}

private[hotfoot] abstract class BasicColumnGenerator[JvmType](
    protected val columnType: ColumnType[JvmType])
  extends ColumnGenerator {

  protected var size: Int = 0

  protected var columnName: String = _

  protected var countGenerated: Int = 0

  protected def initialize() {}

  override def initialize(numVals: Int, columnName: String = ""): Unit = {
    this.size = numVals
    this.columnName = columnName
  }

  override def hasNext: Boolean = countGenerated <= size

  override def generateTo(row: MutableRow, ordinal: Int): Unit = {
    generateSingle(row, ordinal)
  }

  def generateSingle(row: MutableRow, ordinal: Int): Unit = {
    columnType.generate(row, ordinal)
    countGenerated += 1
  }
}

private[hotfoot] class NullColumnGenerator()
  extends BasicColumnGenerator[Any](NULL)
  with NullableColumnGenerator

private[hotfoot] abstract class NativeColumnGenerator[T <: AtomicType](
    override protected val columnType: NativeColumnType[T])
  extends BasicColumnGenerator(columnType)
  with NullableColumnGenerator
//  with CompressibleColumnGenerator[T]

private[hotfoot] class BooleanColumnGenerator()
  extends NativeColumnGenerator(BOOLEAN)

private[hotfoot] class ByteColumnGenerator()
  extends NativeColumnGenerator(BYTE)

private[hotfoot] class ShortColumnGenerator()
  extends NativeColumnGenerator(SHORT)

private[hotfoot] class IntColumnGenerator()
  extends NativeColumnGenerator(INT)

private[hotfoot] class LongColumnGenerator()
  extends NativeColumnGenerator(LONG)

private[hotfoot] class FloatColumnGenerator()
  extends NativeColumnGenerator(FLOAT)

private[hotfoot] class DoubleColumnGenerator()
  extends NativeColumnGenerator(DOUBLE)

private[hotfoot] class StringColumnGenerator()
  extends NativeColumnGenerator(STRING)

private[hotfoot] class BinaryColumnGenerator()
  extends BasicColumnGenerator[Array[Byte]](BINARY)
  with NullableColumnGenerator

private[hotfoot] class CompactDecimalColumnGenerator(dataType: DecimalType)
  extends NativeColumnGenerator(COMPACT_DECIMAL(dataType))

private[hotfoot] class DecimalColumnGenerator(dataType: DecimalType)
  extends BasicColumnGenerator[Decimal](LARGE_DECIMAL(dataType))
  with NullableColumnGenerator

private[hotfoot] class StructColumnGenerator(dataType: StructType)
  extends BasicColumnGenerator[UnsafeRow](STRUCT(dataType))
  with NullableColumnGenerator

private[hotfoot] class ArrayColumnGenerator(dataType: ArrayType)
  extends BasicColumnGenerator[UnsafeArrayData](ARRAY(dataType))
  with NullableColumnGenerator

private[hotfoot] class MapColumnGenerator(dataType: MapType)
  extends BasicColumnGenerator[UnsafeMapData](MAP(dataType))
  with NullableColumnGenerator

object ColumnGenerator {
  val DEFAULT_INITIAL_NUM_VALS = 1000
  var numVals = 0
  def apply(dataType: DataType, vals: Int, columnName: String = ""): ColumnGenerator = {
    numVals = if (vals == 0) DEFAULT_INITIAL_NUM_VALS else vals
    val generator: ColumnGenerator = dataType match {
      case NullType => new NullColumnGenerator
      case BooleanType => new BooleanColumnGenerator
      case ByteType => new ByteColumnGenerator
      case ShortType => new ShortColumnGenerator
      case IntegerType | DateType => new IntColumnGenerator
      case LongType | TimestampType => new LongColumnGenerator
      case FloatType => new FloatColumnGenerator
      case DoubleType => new DoubleColumnGenerator
      case StringType => new StringColumnGenerator
      case BinaryType => new BinaryColumnGenerator
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
        new CompactDecimalColumnGenerator(dt)
      case dt: DecimalType => new DecimalColumnGenerator(dt)
      case struct: StructType => new StructColumnGenerator(struct)
      case array: ArrayType => new ArrayColumnGenerator(array)
      case map: MapType => new MapColumnGenerator(map)
      case udt: UserDefinedType[_] => ColumnGenerator(udt.sqlType, numVals)
      case other =>
        throw new Exception(s"not support type: $other")
    }

    generator.initialize(numVals, columnName)
    generator
  }
}
