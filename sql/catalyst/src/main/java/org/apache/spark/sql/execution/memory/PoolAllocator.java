package org.apache.spark.sql.execution.memory;

import java.util.ArrayList;

public class PoolAllocator implements Allocator {
  private final Allocator allocator;
  private final ArrayList<Buffer> buffers;
  private final int pageSize;
  private Buffer currentBuffer = new Buffer();
  private int currentOffset;

  private static final int PAGE_SIZE = 64 * 1024;

  public PoolAllocator(Allocator allocator) {
    this.pageSize = PAGE_SIZE;
    this.allocator = allocator;
    this.buffers = new ArrayList<>();
    allocator.allocate(pageSize, this.currentBuffer);
    this.buffers.add(this.currentBuffer);
  }

  @Override
  public void allocate(int size, Buffer buffer) {
    if (currentOffset + size > currentBuffer.len) {
      allocator.allocate(Math.max(size, PAGE_SIZE), currentBuffer);
      currentOffset = 0;
    }
    buffer.unsafePtr = currentBuffer.unsafePtr;
    buffer.buffer = currentBuffer.buffer;
    buffer.len = size;
    currentOffset += size;
  }

  @Override
  public void free(Buffer b) {
  }

  @Override
  public void freeAll() {
    for (Buffer buffer: buffers) {
      if (buffer == currentBuffer) continue;
      allocator.free(buffer);
    }
    buffers.clear();
    buffers.add(currentBuffer);
    currentOffset = 0;
  }
}
