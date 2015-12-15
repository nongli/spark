package org.apache.spark.sql.execution.memory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;

public class UnsafeHeapAllocator implements Allocator {
  UnsafeMemoryAllocator allocator = new UnsafeMemoryAllocator();
  @Override
  public void allocate(int size, Buffer buffer) {
    MemoryLocation loc = allocator.allocate(size);
    buffer.unsafePtr = loc.getBaseOffset();
    buffer.buffer = null;
    buffer.len = size;
  }

  @Override
  public void free(Buffer b) {
    allocator.free(new MemoryBlock(null, b.unsafePtr, b.len));
  }

  @Override
  public void freeAll() {

  }
}
