package org.apache.spark.sql.execution;

import org.apache.spark.unsafe.array.ByteArrayMethods;

public class IntHashSet {
  private final int[] data;
  private final byte[] filled;
  private final int mask;

  public IntHashSet(int size) {
    data = new int[(int)ByteArrayMethods.nextPowerOf2(size)];
    filled = new byte[data.length];
    mask = data.length - 1;
  }

  public void put(int key) {
    int bucket = key & mask;
    while (filled[bucket] == 1) {
      bucket++;
      bucket = (bucket + 1) & mask;
    }
    filled[bucket] = 1;
    data[bucket] = key;
  }

  public boolean contains(int key) {
    int bucket = key & mask;
    while (filled[bucket] == 1) {
      if (data[bucket] == key) return true;
      bucket = (bucket + 1) & mask;
    }
    return false;
  }
}
