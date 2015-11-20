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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write data into global row buffer using `UnsafeArrayData` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeArrayWriter {

  private byte[] buffer = new byte[64];
  int cursor = Platform.BYTE_ARRAY_OFFSET;

  // The offset of the global buffer where we start to write this array.
  private int startingOffset;

  public void initialize(int numElements, int fixedElementSize) {
    // We need 4 bytes to store numElements and 4 bytes each element to store offset.
    final int fixedSize = 4 + 4 * numElements;

    grow(fixedSize);
    Platform.putInt(buffer, cursor, numElements);
    cursor += fixedSize;

    // Grows the global buffer ahead for fixed size data.
    grow(fixedElementSize * numElements);
  }

  private long getElementOffset(int ordinal) {
    return startingOffset + 4 + 4 * ordinal;
  }

  public void setNullAt(int ordinal) {
    final int relativeOffset = cursor - startingOffset;
    // Writes negative offset value to represent null element.
    Platform.putInt(buffer, getElementOffset(ordinal), -relativeOffset);
  }

  public void setOffset(int ordinal) {
    final int relativeOffset = cursor - startingOffset;
    Platform.putInt(buffer, getElementOffset(ordinal), relativeOffset);
  }

  public void write(int ordinal, boolean value) {
    Platform.putBoolean(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 1;
  }

  public void write(int ordinal, byte value) {
    Platform.putByte(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 1;
  }

  public void write(int ordinal, short value) {
    Platform.putShort(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 2;
  }

  public void write(int ordinal, int value) {
    Platform.putInt(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 4;
  }

  public void write(int ordinal, long value) {
    Platform.putLong(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 8;
  }

  public void write(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    Platform.putFloat(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 4;
  }

  public void write(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(buffer, cursor, value);
    setOffset(ordinal);
    cursor += 8;
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    if (input.changePrecision(precision, scale)) {
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        Platform.putLong(buffer, cursor, input.toUnscaledLong());
        setOffset(ordinal);
        cursor += 8;
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        assert bytes.length <= 16;
        grow(bytes.length);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, buffer, cursor, bytes.length);
        setOffset(ordinal);
        cursor += bytes.length;
      }
    } else {
      setNullAt(ordinal);
    }
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();

    // grow the global buffer before writing data.
    grow(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(buffer, cursor);

    setOffset(ordinal);

    // move the cursor forward.
    cursor += numBytes;
  }

  public void write(int ordinal, byte[] input) {
    // grow the global buffer before writing data.
    grow(input.length);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(
      input, Platform.BYTE_ARRAY_OFFSET, buffer, cursor, input.length);

    setOffset(ordinal);

    // move the cursor forward.
    cursor += input.length;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(buffer, cursor, input.months);
    Platform.putLong(buffer, cursor + 8, input.microseconds);

    setOffset(ordinal);

    // move the cursor forward.
    cursor += 16;
  }

  private void grow(int neededSize) {
    final int length = cursor - Platform.BYTE_ARRAY_OFFSET + neededSize;
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
    }
  }
}
