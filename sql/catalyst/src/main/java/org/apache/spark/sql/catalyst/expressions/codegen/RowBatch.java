package org.apache.spark.sql.catalyst.expressions.codegen;

import java.util.ArrayList;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class is the in memory representation of rows as they are streamed through operators. It
 * is designed to maximize CPU efficiency and not storage footprint. Since it is expected that
 * each operator allocates one of thee objects, the storage footprint on the task is negligible.
 *
 * The layout is a columnar with values encoded in their native format. Each RowBatch contains
 * a horizontal partitioning of the data, split inot rows.
 *
 * The RowBatch supports either on heap or offheap modes with (mostly) the identical API.
 */
public class RowBatch {

  public static final class Buffer {
    private long offset;
    private int len;
    private byte[] byteData;
    private long address;

    public void set(UTF8String v) {

    }

    public void set(byte[] buffer, int offset, int len) {
      this.byteData = buffer;
      this.offset = offset;
      this.len = len;
      this.address = 0;
    }
  }

  /**
   * This class represents a column of values and provides the main APIs to access the data
   * values. It supports all the types and contains get/put APIs as well as their batched versions.
   * The batched versions are preferable whenever possible.
   *
   * Most of the APIs take the rowId as a parameter. This is the local 0-based row id for values
   * in the current RowBatch.
   */
  public static abstract class Column {
    /**
     * Allocates a column with each element of size `width` either on or off heap.
     */
    public static Column allocate(int capacity, DataType type, boolean offHeap) {
      if (offHeap) {
        return new OffHeapColumn(capacity, type);
      } else {
        return new ArrayColumn(capacity, type);
      }
    }

    /**
     * Returns the off heap ptr for the arrays backing the NULLs and values buffer. Only valid
     * to call for off heap columns.
     */
    public abstract long nullsNativeAddress();
    public abstract long valuesNativeAddress();

    /**
     * Sets the value at rowId to null/not null.
     */
    public abstract void putNotNull(int rowId);
    public abstract void putNull(int rowId);

    /**
     * Sets the values from [rowId, rowId + count) to null/not null.
     */
    public abstract void putNulls(int rowId, int count);
    public abstract void putNotNulls(int rowId, int count);

    /**
     * Sets the value at rowId to the int `value`.
     */
    public abstract void putInt(int rowId, int value);

    /**
     * Sets values from [rowId, rowId + count) to value.
     */
    public abstract void putInts(int rowId, int count, int value);

    /**
     * Sets values from [rowId, rowId + count) to [src + srcOffset, src + srcOffset + count)
     */
    public abstract void putInts(int rowId, int count, int[] src, int srcOffset);

    /**
     * Sets values from [rowId, rowId + count) to [src + srcOffset, src + srcOffset + count)
     * The data in src must be 4-byte little endian ints.
     */
    public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcOffset);

    public abstract void putBuffer(int rowId, byte[] buffer, int offset, int len);
    public abstract void putBufferCopy(int rowId, byte[] buffer, int offset, int len);
    public abstract void getBuffer(int rowId, Buffer buffer);

    public void putUTF8(int rowId, UTF8String v) {
      putBufferCopy(rowId, v.getBytes(), 0, v.numBytes());
    }

    public void getUTF8(int rowId, UTF8String v) {
      getBuffer(rowId, buffer);
      v.setBytes(buffer.byteData, (int)buffer.offset, buffer.len);
    }

    /**
     * Returns whether the value at rowId is NULL.
     */
    public abstract boolean getIsNull(int rowId);

    /**
     * Returns the integer for rowId.
     */
    public abstract int getInt(int rowId);

    /**
     * Returns the number of nulls in the current slice of this column.
     */
    public final int numNulls() { return numNulls; }

    /**
     * Returns true if any of the nulls indicator are set for this column. This can be used
     * as an optimization to prevent setting nulls.
     */
    public final boolean anyNullsSet() { return anyNullsSet; }

    /**
     * Returns the byte width for this column.
     */
    public final int getWidth() { return width; }

    /**
     * Clears all the null indicators for this column.
     */
    public final void clearNulls() {
      putNotNulls(0, capacity);
      anyNullsSet = false;
    }

    /**
     * Resets this column for writing. The currently stored values are no longer accessible.
     */
    public void reset() {
      numNulls = 0;
    }

    /**
     * Maximum number of rows that can be stored in this column.
     */
    protected final int capacity;

    /**
     * Byte width fo this column.
     */
    protected final int width;

    /**
     * Number of nulls in this column.
     */
    protected int numNulls;

    protected Buffer buffer = new Buffer();

    /**
     * True if there is at least one NULL byte set. TODO: explain this better.
     */
    protected boolean anyNullsSet;

    protected  final DataType type;

    protected Column(int capacity, DataType type) {
      this.capacity = capacity;
      this.type = type;
      this.width = type.defaultSize();
    }
  }

  /**
   * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
   * and a java array for the values.
   */
  private final static class ArrayColumn extends Column {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    // This is faster than a boolean array and we optimize this over memory footprint.
    private final byte[] nulls;

    // State to store int values.
    private int[] intData;

    // State to store byte buffer columns.
    private byte[][] byteData;
    private int[] lenData;
    private int[] offsetData;

    // State used if the byte buffer should be copied.
    private ArrayList<byte[]> byteBuffers;
    private byte[] currentBuffer;
    private int currentBufferOffset;

    public ArrayColumn(int capacity, DataType type) {
      super(capacity, type);
      if (type instanceof IntegerType || type instanceof DecimalType) {
        this.intData = new int[capacity];
      } else if (type instanceof StringType) {
        this.byteData = new byte[capacity][];
        this.lenData = new int[capacity];
        this.offsetData = new int[capacity];
        byteBuffers = new ArrayList<>();
        currentBuffer = new byte[DEFAULT_BUFFER_SIZE];
        byteBuffers.add(currentBuffer);
        currentBufferOffset = 0;
      } else {
        throw new RuntimeException("Unhandled " + type);
      }
      this.nulls = new byte[capacity];
      reset();
    }

    public long valuesNativeAddress() {
      throw new RuntimeException("Cannot get native address for on heap column");
    }
    public long nullsNativeAddress() {
      throw new RuntimeException("Cannot get native address for on heap column");
    }

    public void putNotNull(int rowId) {
      nulls[rowId] = (byte)0;
    }

    public void putNull(int rowId) {
      nulls[rowId] = (byte)1;
      ++numNulls;
      anyNullsSet = true;
    }

    public void putNulls(int rowId, int count) {
      for (int i = 0; i < count; ++i) {
        nulls[rowId + i] = (byte)1;
      }
      anyNullsSet = true;
      numNulls += count;
    }

    public void putNotNulls(int rowId, int count) {
      for (int i = 0; i < count; ++i) {
        nulls[rowId + i] = (byte)0;
      }
    }

    public void putInt(int rowId, int value) {
      intData[rowId] = value;
    }

    public void putInts(int rowId, int count, int value) {
      for (int i = 0; i < count; ++i) {
        intData[i + rowId] = value;
      }
    }

    public void putInts(int rowId, int count, int[] src, int srcOffset) {
      System.arraycopy(src, srcOffset, intData, rowId, count);
    }

    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcOffset) {
      for (int i = 0; i < count; ++i) {
        intData[i + rowId] = Platform.getInt(src, srcOffset);
        srcOffset += 4;
      }
    }

    public void putBuffer(int rowId, byte[] src, int offset, int len) {
      byteData[rowId] = src;
      lenData[rowId] = len;
      offsetData[rowId] = offset;
    }

    public void putBufferCopy(int rowId, byte[] src, int offset, int len) {
      if (currentBufferOffset + len > currentBuffer.length) {
        currentBuffer = new byte[Math.min(DEFAULT_BUFFER_SIZE, len)];
        byteBuffers.add(currentBuffer);
        currentBufferOffset = 0;
      }
      System.arraycopy(src, offset, currentBuffer, currentBufferOffset, len);
      byteData[rowId] = currentBuffer;
      lenData[rowId] = len;
      offsetData[rowId] = currentBufferOffset;
      currentBufferOffset += len;
    }

    public void getBuffer(int rowId, Buffer buffer) {
      buffer.byteData = byteData[rowId];
      buffer.len = lenData[rowId];
      buffer.offset = offsetData[rowId];
    }

    public boolean getIsNull(int rowId) {
      return nulls[rowId] == 1;
    }

    public int getInt(int rowId) {
      return intData[rowId];
    }

    public void reset() {
      super.reset();
      if (byteBuffers != null && byteBuffers.size() > 1) {
        int size = 0;
        for (int i = 0; i < byteBuffers.size(); ++i) {
          size += byteBuffers.get(i).length;
        }
        byteBuffers.clear();
        currentBuffer = new byte[size];
        byteBuffers.add(currentBuffer);
      }
      currentBufferOffset = 0;
    }
  }

  private final static class OffHeapColumn extends Column {
    private final long nulls;
    private final long data;

    private OffHeapColumn(int capacity, DataType type) {
      super(capacity, type);
      this.nulls = Platform.allocateMemory(capacity);
      if (type instanceof IntegerType || type instanceof DecimalType) {
        this.data = Platform.allocateMemory(capacity * 4);
      } else {
        throw new RuntimeException("Unhandled " + type);
      }
      clearNulls();
      reset();
    }

    public long valuesNativeAddress() { return data; }
    public long nullsNativeAddress() { return nulls; }

    public void putNotNull(int rowId) {
      Platform.putByte(null, nulls + rowId, (byte)0);
    }

    public void putNull(int rowId) {
      Platform.putByte(null, nulls + rowId, (byte)1);
      ++numNulls;
      anyNullsSet = true;
    }

    public void putNulls(int rowId, int count) {
      long offset = nulls + rowId;
      for (int i = 0; i < count; ++i, ++offset) {
        Platform.putByte(null, offset, (byte)1);
      }
      anyNullsSet = true;
      numNulls += count;
    }

    public void putNotNulls(int rowId, int count) {
      long offset = nulls + rowId;
      for (int i = 0; i < count; ++i, ++offset) {
        Platform.putByte(null, offset, (byte)0);
      }
    }

    public boolean getIsNull(int rowId) {
      return Platform.getByte(null, nulls + rowId) == 1;
    }

    public void putInt(int rowId, int value) {
      Platform.putInt(null, data + 4 * rowId, value);
    }

    public void putInts(int rowId, int count, int value) {
      long offset = data + 4 * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putInt(null, offset, value);
      }
    }

    public void putInts(int rowId, int count, int[] src, int srcOffset) {
      Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcOffset * 4,
          null, data + 4 * rowId, count * 4);
    }

    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcOffset) {
      Platform.copyMemory(src, srcOffset + Platform.BYTE_ARRAY_OFFSET,
          null, data + 4 * rowId, count * 4);
    }

    public int getInt(int rowId) {
      return Platform.getInt(null, data + 4 * rowId);
    }

    public void putBuffer(int rowId, byte[] src, int offset, int len) {
    }
    public void putBufferCopy(int rowId, byte[] src, int offset, int len) {
    }
    public void getBuffer(int rowId, Buffer buffer) {
    }
  }

  public static RowBatch allocate(StructType schema) {
    return allocate(schema, 32 * 1024);
  }
  public static RowBatch allocate(StructType schema, int maxRows) {
    return new RowBatch(schema, maxRows, false);
  }

  public static RowBatch allocateOffHeap(StructType schema) {
    return allocateOffHeap(schema, 32 * 1024);
  }
  public static RowBatch allocateOffHeap(StructType schema, int maxRows) {
    return new RowBatch(schema, maxRows, true);
  }

  private final StructType schema;
  private final int capacity;
  private int numRows = 0;
  private final Column[] columns;

  private RowBatch(StructType schema, int maxRows, boolean offHeap) {
    this.schema = schema;
    this.capacity = maxRows;
    this.columns = new Column[schema.size()];

    for (int i = 0; i < schema.fields().length; ++i) {
      StructField field = schema.fields()[i];
      DataType dt = field.dataType();
      columns[i] = Column.allocate(maxRows, dt, offHeap);
    }
  }

  public void putNull(int ordinal, int rowId, boolean b) { columns[ordinal].putNull(rowId); }
  public void putNotNull(int ordinal, int rowId, boolean b) { columns[ordinal].putNotNull(rowId); }
  public void putInt(int ordinal, int rowId, int value) { columns[ordinal].putInt(rowId, value); }

  public boolean getIsNull(int ordinal, int rowId) { return columns[ordinal].getIsNull(rowId); }
  public int getInt(int ordinal, int rowId) { return columns[ordinal].getInt(rowId); }

  public void reset(boolean read) {
    for (int i = 0; i < numCols(); ++i) {
      columns[i].reset();
    }
    if (!read) this.numRows = 0;
  }

  public void setNumRows(int numRows) { this.numRows = numRows; }
  public int numCols() { return columns.length; }
  public int numRows() { return numRows; }
  public int capacity() { return capacity; }
  public Column column(int ordinal) { return columns[ordinal]; }
}
