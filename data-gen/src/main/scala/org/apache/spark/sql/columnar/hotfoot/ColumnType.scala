package org.apache.spark.sql.columnar.hotfoot

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteBuffer

import org.apache.spark.unsafe.Platform

import scala.reflect.runtime.universe.{TypeTag, runtimeMirror}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.execution.columnar.MutableUnsafeRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.util.Random

/**
 * An abstract class that represents type of a column. Used to generate objects of a column.
 *
 * @tparam JvmType Underlying Java type to represent the elements.
 */
sealed abstract class ColumnType[JvmType] {

  val random = new Random()

  // The catalyst data type of this column
  def dataType: DataType

  // Default size in bytes for one element of type T (e.g. 4 for `Int`).
  def defaultSize: Int

  /**
   * Extracts a value out of the buffer at the buffer's current position.
   */
  def extract(buffer: ByteBuffer): JvmType

  /**
   * Generate random value for this type of column
   * @return
   */
  def generate(): JvmType

  def generate(row: MutableRow, ordinal: Int): Unit = {
    setField(row, ordinal, generate())
  }
  /**
   * Extracts a value out of the buffer at the buffer's current position and stores in
   * `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs whenever
   * possible.
   */
  def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    setField(row, ordinal, extract(buffer))
  }

  /**
   * Appends the given value v of type T into the given ByteBuffer.
   */
  def append(v: JvmType, buffer: ByteBuffer): Unit

  /**
   * Appends `row(ordinal)` of type T into the given ByteBuffer. Subclasses should override this
   * method to avoid boxing/unboxing costs whenever possible.
   */
  def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    append(getField(row, ordinal), buffer)
  }

  /**
   * Returns the size of the value `row(ordinal)`. This is used to calculate the size of variable
   * length types such as byte arrays and strings.
   */
  def actualSize(row: InternalRow, ordinal: Int): Int = defaultSize

  /**
   * Returns `row(ordinal)`. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   */
  def getField(row: InternalRow, ordinal: Int): JvmType

  /**
   * Sets `row(ordinal)` to `field`. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   */
  def setField(row: MutableRow, ordinal: Int, value: JvmType): Unit

  /**
   * Copies `from(fromOrdinal)` to `to(toOrdinal)`. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   */
  def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int): Unit = {
    to.update(toOrdinal, from.get(fromOrdinal, dataType))
  }

  /**
   * Creates a duplicated copy of the value.
   */
  def clone(v: JvmType): JvmType = v

  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

object NULL extends ColumnType[Any] {

  override def generate: Any = null
  override def dataType: DataType = NullType
  override def defaultSize: Int = 0
  override def append(v: Any, buffer: ByteBuffer): Unit = {}
  override def extract(buffer: ByteBuffer): Any = null
  override def setField(row: MutableRow, ordinal: Int, value: Any): Unit = row.setNullAt(ordinal)
  override def getField(row: InternalRow, ordinal: Int): Any = null
}

abstract class NativeColumnType[T <: AtomicType](
    val dataType: T,
    val defaultSize: Int)
  extends ColumnType[T#InternalType] {

  /**
   * Scala TypeTag. Can be used to create primitive arrays and hash tables.
   */
  def scalaTag: TypeTag[dataType.InternalType] = dataType.tag
}


object INT extends NativeColumnType(IntegerType, 4) {
  override def append(v: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putInt(row.getInt(ordinal))
  }

  override def extract(buffer: ByteBuffer): Int = {
    buffer.getInt()
  }

  override def generate(): Int = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextInt()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setInt(ordinal, buffer.getInt())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Int): Unit = {
    row.setInt(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Int = row.getInt(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setInt(toOrdinal, from.getInt(fromOrdinal))
  }
}

object LONG extends NativeColumnType(LongType, 8) {
  override def append(v: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putLong(row.getLong(ordinal))
  }

  override def extract(buffer: ByteBuffer): Long = {
    buffer.getLong()
  }

  override def generate(): Long = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextLong()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setLong(ordinal, buffer.getLong())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Long): Unit = {
    row.setLong(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Long = row.getLong(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setLong(toOrdinal, from.getLong(fromOrdinal))
  }
}

object FLOAT extends NativeColumnType(FloatType, 4) {
  override def append(v: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putFloat(row.getFloat(ordinal))
  }

  override def extract(buffer: ByteBuffer): Float = {
    buffer.getFloat()
  }

  override def generate(): Float = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextFloat()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setFloat(ordinal, buffer.getFloat())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Float): Unit = {
    row.setFloat(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Float = row.getFloat(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setFloat(toOrdinal, from.getFloat(fromOrdinal))
  }
}

object DOUBLE extends NativeColumnType(DoubleType, 8) {
  override def append(v: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putDouble(row.getDouble(ordinal))
  }

  override def extract(buffer: ByteBuffer): Double = {
    buffer.getDouble()
  }

  override def generate(): Double = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextDouble()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setDouble(ordinal, buffer.getDouble())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Double): Unit = {
    row.setDouble(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Double = row.getDouble(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setDouble(toOrdinal, from.getDouble(fromOrdinal))
  }
}

object BOOLEAN extends NativeColumnType(BooleanType, 1) {
  override def append(v: Boolean, buffer: ByteBuffer): Unit = {
    buffer.put(if (v) 1: Byte else 0: Byte)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(if (row.getBoolean(ordinal)) 1: Byte else 0: Byte)
  }

  override def extract(buffer: ByteBuffer): Boolean = buffer.get() == 1

  override def generate(): Boolean = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextBoolean()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setBoolean(ordinal, buffer.get() == 1)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Boolean): Unit = {
    row.setBoolean(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setBoolean(toOrdinal, from.getBoolean(fromOrdinal))
  }
}

object BYTE extends NativeColumnType(ByteType, 1) {
  override def append(v: Byte, buffer: ByteBuffer): Unit = {
    buffer.put(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.put(row.getByte(ordinal))
  }

  override def extract(buffer: ByteBuffer): Byte = {
    buffer.get()
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setByte(ordinal, buffer.get())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Byte): Unit = {
    row.setByte(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Byte = row.getByte(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setByte(toOrdinal, from.getByte(fromOrdinal))
  }

  override def generate(): Byte = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    val length = 1
    val bytes = new Array[Byte](length)
    random.nextBytes(bytes)
    bytes(0)
  }
}

object SHORT extends NativeColumnType(ShortType, 2) {
  override def append(v: Short, buffer: ByteBuffer): Unit = {
    buffer.putShort(v)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    buffer.putShort(row.getShort(ordinal))
  }

  override def extract(buffer: ByteBuffer): Short = {
    buffer.getShort()
  }

  override def generate(): Short = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    random.nextInt(Short.MaxValue + 1).toShort
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    row.setShort(ordinal, buffer.getShort())
  }

  override def setField(row: MutableRow, ordinal: Int, value: Short): Unit = {
    row.setShort(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Short = row.getShort(ordinal)

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    to.setShort(toOrdinal, from.getShort(fromOrdinal))
  }
}

/**
 * A fast path to copy var-length bytes between ByteBuffer and UnsafeRow without creating wrapper
 * objects.
 */
private[columnar] trait DirectCopyColumnType[JvmType] extends ColumnType[JvmType] {

  // copy the bytes from ByteBuffer to UnsafeRow
  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      val numBytes = buffer.getInt
      val cursor = buffer.position()
      buffer.position(cursor + numBytes)
      row.asInstanceOf[MutableUnsafeRow].writer.write(ordinal, buffer.array(),
        buffer.arrayOffset() + cursor, numBytes)
    } else {
      setField(row, ordinal, extract(buffer))
    }
  }

  // copy the bytes from UnsafeRow to ByteBuffer
  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    if (row.isInstanceOf[UnsafeRow]) {
      row.asInstanceOf[UnsafeRow].writeFieldTo(ordinal, buffer)
    } else {
      super.append(row, ordinal, buffer)
    }
  }
}

object STRING extends NativeColumnType(StringType, 8) {
  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getString(ordinal).getBytes("utf-8").length + 4
  }

  override def append(v: UTF8String, buffer: ByteBuffer): Unit = {
    val stringBytes = v.getBytes
    buffer.putInt(stringBytes.length).put(stringBytes, 0, stringBytes.length)
  }

  override def extract(buffer: ByteBuffer): UTF8String = {
    val length = buffer.getInt()
    val stringBytes = new Array[Byte](length)
    buffer.get(stringBytes, 0, length)
    UTF8String.fromBytes(stringBytes)
  }

  override def generate(): UTF8String = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    val length = random.nextInt()
    UTF8String.fromString(random.nextString(length))
  }

  override def setField(row: MutableRow, ordinal: Int, value: UTF8String): Unit = {
    row.update(ordinal, value.clone())
  }

  override def getField(row: InternalRow, ordinal: Int): UTF8String = {
    row.getUTF8String(ordinal)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }
}

private[hotfoot] case class COMPACT_DECIMAL(precision: Int, scale: Int)
  extends NativeColumnType(DecimalType(precision, scale), 8) {

  override def generate(): Decimal = {
    Decimal(random.nextLong(), precision, scale)
  }

  override def extract(buffer: ByteBuffer): Decimal = {
    Decimal(buffer.getLong(), precision, scale)
  }

  override def extract(buffer: ByteBuffer, row: MutableRow, ordinal: Int): Unit = {
    if (row.isInstanceOf[MutableUnsafeRow]) {
      // copy it as Long
      row.setLong(ordinal, buffer.getLong())
    } else {
      setField(row, ordinal, extract(buffer))
    }
  }

  override def append(v: Decimal, buffer: ByteBuffer): Unit = {
    buffer.putLong(v.toUnscaledLong)
  }

  override def append(row: InternalRow, ordinal: Int, buffer: ByteBuffer): Unit = {
    if (row.isInstanceOf[UnsafeRow]) {
      // copy it as Long
      buffer.putLong(row.getLong(ordinal))
    } else {
      append(getField(row, ordinal), buffer)
    }
  }

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def copyField(from: InternalRow, fromOrdinal: Int, to: MutableRow, toOrdinal: Int) {
    setField(to, toOrdinal, getField(from, fromOrdinal))
  }
}

private[columnar] object COMPACT_DECIMAL {
  def apply(dt: DecimalType): COMPACT_DECIMAL = {
    COMPACT_DECIMAL(dt.precision, dt.scale)
  }
}

private[hotfoot] sealed abstract class ByteArrayColumnType[JvmType](val defaultSize: Int)
  extends ColumnType[JvmType] with DirectCopyColumnType[JvmType] {

  def serialize(value: JvmType): Array[Byte]
  def deserialize(bytes: Array[Byte]): JvmType

  override def append(v: JvmType, buffer: ByteBuffer): Unit = {
    val bytes = serialize(v)
    buffer.putInt(bytes.length).put(bytes, 0, bytes.length)
  }

  override def extract(buffer: ByteBuffer): JvmType = {
    val length = buffer.getInt()
    val bytes = new Array[Byte](length)
    buffer.get(bytes, 0, length)
    deserialize(bytes)
  }

  override def generate(): JvmType = {
    // generate a random value ?
    // TODO honor the user rules while generating value ?
    val length = random.nextInt()
    val bytes = new Array[Byte](length)
    random.nextBytes(bytes)
    deserialize(bytes)
  }
}

object BINARY extends ByteArrayColumnType[Array[Byte]](16) {

  def dataType: DataType = BinaryType

  override def setField(row: MutableRow, ordinal: Int, value: Array[Byte]): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): Array[Byte] = {
    row.getBinary(ordinal)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    row.getBinary(ordinal).length + 4
  }

  def serialize(value: Array[Byte]): Array[Byte] = value
  def deserialize(bytes: Array[Byte]): Array[Byte] = bytes
}

private[hotfoot] case class LARGE_DECIMAL(precision: Int, scale: Int)
  extends ByteArrayColumnType[Decimal](12) {

  override val dataType: DataType = DecimalType(precision, scale)

  override def generate(): Decimal = {
    val length = random.nextInt()
    val bytes = new Array[Byte](length)
    random.nextBytes(bytes)
    val javaDecimal = new BigDecimal(new BigInteger(bytes), scale)
    Decimal.apply(javaDecimal, precision, scale)
  }

  override def getField(row: InternalRow, ordinal: Int): Decimal = {
    row.getDecimal(ordinal, precision, scale)
  }

  override def setField(row: MutableRow, ordinal: Int, value: Decimal): Unit = {
    row.setDecimal(ordinal, value, precision)
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).toJavaBigDecimal.unscaledValue().bitLength() / 8 + 1
  }

  override def serialize(value: Decimal): Array[Byte] = {
    value.toJavaBigDecimal.unscaledValue().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): Decimal = {
    val javaDecimal = new BigDecimal(new BigInteger(bytes), scale)
    Decimal.apply(javaDecimal, precision, scale)
  }
}

object LARGE_DECIMAL {
  def apply(dt: DecimalType): LARGE_DECIMAL = {
    LARGE_DECIMAL(dt.precision, dt.scale)
  }
}

private[hotfoot] case class STRUCT(dataType: StructType)
  extends ColumnType[UnsafeRow] with DirectCopyColumnType[UnsafeRow] {

  private val numOfFields: Int = dataType.fields.size

  override def defaultSize: Int = 20

  override def generate(): UnsafeRow = {
    // TODO: How to generate values ?
    new UnsafeRow
  }

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeRow): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeRow = {
    row.getStruct(ordinal, numOfFields).asInstanceOf[UnsafeRow]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    4 + getField(row, ordinal).getSizeInBytes
  }

  override def append(value: UnsafeRow, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeRow = {
    val sizeInBytes = buffer.getInt()
    assert(buffer.hasArray)
    val cursor = buffer.position()
    buffer.position(cursor + sizeInBytes)
    val unsafeRow = new UnsafeRow
    unsafeRow.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      numOfFields,
      sizeInBytes)
    unsafeRow
  }

  override def clone(v: UnsafeRow): UnsafeRow = v.copy()
}

private[columnar] case class ARRAY(dataType: ArrayType)
  extends ColumnType[UnsafeArrayData] with DirectCopyColumnType[UnsafeArrayData] {

  override def defaultSize: Int = 16

  override def generate(): UnsafeArrayData = {
    // TODO: How to generate values
    new UnsafeArrayData
  }

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeArrayData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeArrayData = {
    row.getArray(ordinal).asInstanceOf[UnsafeArrayData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeArray = getField(row, ordinal)
    4 + unsafeArray.getSizeInBytes
  }

  override def append(value: UnsafeArrayData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeArrayData = {
    val numBytes = buffer.getInt
    assert(buffer.hasArray)
    val cursor = buffer.position()
    buffer.position(cursor + numBytes)
    val array = new UnsafeArrayData
    array.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      numBytes)
    array
  }

  override def clone(v: UnsafeArrayData): UnsafeArrayData = v.copy()
}

private[hotfoot] case class MAP(dataType: MapType)
  extends ColumnType[UnsafeMapData] with DirectCopyColumnType[UnsafeMapData] {

  override def defaultSize: Int = 32

  override def generate(): UnsafeMapData = {
    // TODO: How to generate values
    new UnsafeMapData
  }

  override def setField(row: MutableRow, ordinal: Int, value: UnsafeMapData): Unit = {
    row.update(ordinal, value)
  }

  override def getField(row: InternalRow, ordinal: Int): UnsafeMapData = {
    row.getMap(ordinal).asInstanceOf[UnsafeMapData]
  }

  override def actualSize(row: InternalRow, ordinal: Int): Int = {
    val unsafeMap = getField(row, ordinal)
    4 + unsafeMap.getSizeInBytes
  }

  override def append(value: UnsafeMapData, buffer: ByteBuffer): Unit = {
    buffer.putInt(value.getSizeInBytes)
    value.writeTo(buffer)
  }

  override def extract(buffer: ByteBuffer): UnsafeMapData = {
    val numBytes = buffer.getInt
    val cursor = buffer.position()
    buffer.position(cursor + numBytes)
    val map = new UnsafeMapData
    map.pointTo(
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + cursor,
      numBytes)
    map
  }

  override def clone(v: UnsafeMapData): UnsafeMapData = v.copy()
}

object ColumnType {
  def apply(dataType: DataType): ColumnType[_] = {
    dataType match {
      case NullType => NULL
      case BooleanType => BOOLEAN
      case ByteType => BYTE
      case ShortType => SHORT
      case IntegerType | DateType => INT
      case LongType | TimestampType => LONG
      case FloatType => FLOAT
      case DoubleType => DOUBLE
      case StringType => STRING
      case BinaryType => BINARY
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => COMPACT_DECIMAL(dt)
      case dt: DecimalType => LARGE_DECIMAL(dt)
      case arr: ArrayType => ARRAY(arr)
      case map: MapType => MAP(map)
      case struct: StructType => STRUCT(struct)
      case udt: UserDefinedType[_] => apply(udt.sqlType)
      case other =>
        throw new Exception(s"Unsupported type: $other")
    }
  }
}
