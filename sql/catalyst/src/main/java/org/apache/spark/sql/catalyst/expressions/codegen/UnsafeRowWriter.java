/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write data into global row buffer using `UnsafeRow` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeRowWriter {
  private final int nullBitsSize;
  private final int fixedSize;

  private final UnsafeRow row;

  // Buffer backing row.
  private byte[] buffer;

  // The current offset to append var len data to.
  private int varDataCursor;


  public UnsafeRowWriter(UnsafeRow row, int numFields) {
    this.row = row;
    this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);

    // grow the global buffer to make sure it has enough space to write fixed-length data.
    fixedSize = nullBitsSize + 8 * numFields;
    buffer = new byte[fixedSize];
    growBuffer(fixedSize);
    row.pointTo(buffer, numFields, fixedSize);
  }

  /**
   * This should be called once for every row to reset state to write a new row.
   */
  public void reset() {
    // zero-out the null bits region
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET + i, 0L);
    }
    varDataCursor = Platform.BYTE_ARRAY_OFFSET + fixedSize;
  }

  /**
   * Must be called at the end of each row if it contains var len data.
   */
  public void finalizeRow() {
    row.setSize(varDataCursor - Platform.BYTE_ARRAY_OFFSET);
  }

  public boolean isNullAt(int ordinal) {
    return BitSetMethods.isSet(buffer, Platform.BYTE_ARRAY_OFFSET, ordinal);
  }

  public void setNullAt(int ordinal) {
    BitSetMethods.set(buffer, Platform.BYTE_ARRAY_OFFSET, ordinal);
    Platform.putLong(buffer, getFieldOffset(ordinal), 0L);
  }

  public void write(int ordinal, boolean value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer, offset, 0L);
    Platform.putBoolean(buffer, offset, value);
  }

  public void write(int ordinal, byte value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer, offset, 0L);
    Platform.putByte(buffer, offset, value);
  }

  public void write(int ordinal, short value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer, offset, 0L);
    Platform.putShort(buffer, offset, value);
  }

  public void write(int ordinal, int value) {
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer, offset, 0L);
    Platform.putInt(buffer, offset, value);
  }

  public void write(int ordinal, long value) {
    Platform.putLong(buffer, getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    final long offset = getFieldOffset(ordinal);
    Platform.putLong(buffer, offset, 0L);
    Platform.putFloat(buffer, offset, value);
  }

  public void write(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(buffer, getFieldOffset(ordinal), value);
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // make sure Decimal object has the same scale as DecimalType
      if (input.changePrecision(precision, scale)) {
        Platform.putLong(buffer, getFieldOffset(ordinal), input.toUnscaledLong());
      } else {
        setNullAt(ordinal);
      }
    } else {
      // grow the global buffer before writing data.
      growBuffer(16);

      // zero-out the bytes
      Platform.putLong(buffer, varDataCursor, 0L);
      Platform.putLong(buffer, varDataCursor + 8, 0L);

      // Make sure Decimal object has the same scale as DecimalType.
      // Note that we may pass in null Decimal object to set null for it.
      if (input == null || !input.changePrecision(precision, scale)) {
        BitSetMethods.set(buffer, Platform.BYTE_ARRAY_OFFSET, ordinal);
        // keep the offset for future update
        setOffsetAndSize(ordinal, 0L);
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        assert bytes.length <= 16;

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, buffer, varDataCursor, bytes.length);
        setOffsetAndSize(ordinal, bytes.length);
      }

      // move the cursor forward.
      varDataCursor += 16;
    }
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

    // grow the global buffer before writing data.
    growBuffer(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(buffer, varDataCursor);

    setOffsetAndSize(ordinal, numBytes);

    // move the cursor forward.
    varDataCursor += roundedSize;
  }

  public void write(int ordinal, byte[] input) {
    write(ordinal, input, 0, input.length);
  }

  public void write(int ordinal, byte[] input, int offset, int numBytes) {
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);

    // grow the global buffer before writing data.
    growBuffer(roundedSize);

    zeroOutPaddingBytes(numBytes);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(input, Platform.BYTE_ARRAY_OFFSET + offset,
      buffer, varDataCursor, numBytes);

    setOffsetAndSize(ordinal, numBytes);

    // move the cursor forward.
    varDataCursor+= roundedSize;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    growBuffer(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(buffer, varDataCursor, input.months);
    Platform.putLong(buffer, varDataCursor + 8, input.microseconds);

    setOffsetAndSize(ordinal, 16);

    // move the cursor forward.
    varDataCursor += 16;
  }

  private long getFieldOffset(int ordinal) {
    return Platform.BYTE_ARRAY_OFFSET + nullBitsSize + 8 * ordinal;
  }

  private void setOffsetAndSize(int ordinal, long size) {
    setOffsetAndSize(ordinal, varDataCursor, size);
  }

  private void setOffsetAndSize(int ordinal, long currentCursor, long size) {
    final long relativeOffset = currentCursor - Platform.BYTE_ARRAY_OFFSET;
    final long fieldOffset = getFieldOffset(ordinal);
    final long offsetAndSize = (relativeOffset << 32) | size;

    Platform.putLong(buffer, fieldOffset, offsetAndSize);
  }

  private void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(buffer, varDataCursor + ((numBytes >> 3) << 3), 0L);
    }
  }

  private void growBuffer(int neededSize) {
    final int length = varDataCursor - Platform.BYTE_ARRAY_OFFSET + neededSize;
    if (buffer.length < length) {
      // This will not happen frequently, because the buffer is re-used.
      final byte[] tmp = new byte[length * 2];
      Platform.copyMemory(
          buffer,
          Platform.BYTE_ARRAY_OFFSET,
          tmp,
          Platform.BYTE_ARRAY_OFFSET,
          length);
      buffer = tmp;
      row.pointTo(buffer, length * 2);
    }
  }
}
