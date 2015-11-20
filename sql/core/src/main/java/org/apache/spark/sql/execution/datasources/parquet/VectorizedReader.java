package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.catalyst.expressions.codegen.RowBatch;

public interface VectorizedReader {
  void readInts(int total, RowBatch.Column c, int rowId);
  int readInteger();
  void skip(int n);
}
