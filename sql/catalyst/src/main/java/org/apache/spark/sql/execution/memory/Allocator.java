package org.apache.spark.sql.execution.memory;

interface Allocator {
  final class Buffer {
    public long unsafePtr;
    public byte[] buffer;
    int len;
  }

  void allocate(int size, Buffer buffer);

  void free(Buffer b);

  void freeAll();
}
