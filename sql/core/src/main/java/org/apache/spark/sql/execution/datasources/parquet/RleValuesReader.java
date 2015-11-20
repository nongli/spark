package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.catalyst.expressions.codegen.RowBatch;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

public final class RleValuesReader extends ValuesReader implements VectorizedReader {
  enum MODE {
    RLE,
    PACKED
  }

  private MODE mode;
  int bitWidth;
  int bytesWidth;
  private int end;
  private byte[] in;
  private int offset;

  private BytePacker packer;

  private int currentCount;
  private int currentValue;
  private int[] currentBuffer = new int[16];
  private int currentBufferIdx = 0;

  private final boolean fixedWidth;

  public RleValuesReader(int bitWidth) {
    fixedWidth = true;
    init(bitWidth);
  }

  public RleValuesReader() {
    fixedWidth = false;
  }

  public RleValuesReader(int bitWidth, byte[] page, int start) {
    fixedWidth = true;
    init(bitWidth);
    this.in = page;
    this.offset = start;
    this.end = page.length;
  }

  @Override
  public void initFromPage(int valueCount, byte[] page, int start) {
    this.offset = start;
    this.in = page;
    if (fixedWidth) {
      int length = readIntLittleEndian();
      this.end = this.offset + length;
    } else {
      this.end = page.length;
      if (this.end != this.offset) init(page[this.offset++] & 255);
    }
    this.currentCount = 0;
  }

  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }

  public int getNextOffset() {
    return this.end;
  }

  public boolean readBoolean() {
    return this.readInteger() != 0;
  }

  public void skip() {
    this.readInteger();
  }

  public void skip(int num) {
    while (num > 0) {
      if (this.currentCount == 0) this.readNext();
      int n = Math.min(num, this.currentCount);
      switch (mode) {
        case RLE:
          break;
        case PACKED:
          currentBufferIdx += n;
          break;
      }
      num -= n;
      currentCount -= n;
    }
  }

  @Override
  public int readValueDictionaryId() {
    return readInteger();
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) { this.readNext(); }

    --this.currentCount;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.currentBuffer[currentBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  /**
   * Reads `total` ints into `c` filling them in starting at `c[rowId]`
   */
  public void readInts(int total, RowBatch.Column c, int rowId) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNext();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          c.putInts(rowId, n, currentValue);
          break;
        case PACKED:
          c.putInts(rowId, n, currentBuffer, currentBufferIdx);
          currentBufferIdx += n;
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  /**
   * Reads `total` ints into `c` filling them in starting at `c[rowId]`. This reader
   * reads the definition levels and then will read from `data` for the non-null values.
   * If the value is null, c will be populated with `nullValue`.
   *
   * This is a batched version of this logic:
   *  if (this.readInt() == level) {
   *    c[rowId] = data.readInteger();
   *  } else {
   *    c[rowId] = nullValue;
   *  }
   */
  public void readInts(int total, RowBatch.Column c, int rowId, int level,
                       VectorizedReader data, int nullValue) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNext();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readInts(n, c, rowId);
            c.putNotNulls(rowId, n);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putInt(rowId + i, data.readInteger());
              c.putNotNull(rowId + i);
            } else {
              c.putInt(rowId + i, nullValue);
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  private int readUnsignedVarInt() {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in[offset++] & 255;
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  private int readIntLittleEndian() {
    int ch4 = in[offset] & 255;
    int ch3 = in[offset + 1] & 255;
    int ch2 = in[offset + 2] & 255;
    int ch1 = in[offset + 3] & 255;
    offset += 4;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  private int readIntLittleEndianPaddedOnBitWidth() {
    switch(bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in[offset++] & 255;
      case 2: {
        int ch2 = in[offset] & 255;
        int ch1 = in[offset + 1] & 255;
        offset += 2;
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = in[offset] & 255;
        int ch2 = in[offset + 1] & 255;
        int ch1 = in[offset + 2] & 255;
        offset += 3;
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

  private void readNext()  {
    Preconditions.checkArgument(this.offset < this.end,
        "Reading past RLE/BitPacking stream. offset=" + this.offset + " end=" + this.end);
    int header = readUnsignedVarInt();
    this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
      case RLE:
        this.currentCount = header >>> 1;
        this.currentValue = readIntLittleEndianPaddedOnBitWidth();
        return;
      case PACKED:
        int numGroups = header >>> 1;
        this.currentCount = numGroups * 8;

        if (this.currentBuffer.length < this.currentCount) {
          this.currentBuffer = new int[this.currentCount];
        }
        currentBufferIdx = 0;
        int bytesToRead = (int)Math.ceil((double)(this.currentCount * this.bitWidth) / 8.0D);

        bytesToRead = Math.min(bytesToRead, this.end - this.offset);
        int valueIndex = 0;
        for (int byteIndex = offset; valueIndex < this.currentCount; byteIndex += this.bitWidth) {
          this.packer.unpack8Values(in, byteIndex, this.currentBuffer, valueIndex);
          valueIndex += 8;
        }
        offset += bytesToRead;
        return;
      default:
        throw new ParquetDecodingException("not a valid mode " + this.mode);
    }
  }
}
