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

package org.apache.spark.sql.execution.joins

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.mutable


class HashedRelationSuite extends SparkFunSuite with SharedSQLContext {

  // Key is simply the record itself
  private val keyProjection = new Projection {
    override def apply(row: InternalRow): InternalRow = row
  }

  private val unsafeKeyProjection = new UnsafeProjection {
    override def apply(row: InternalRow): UnsafeRow = row.asInstanceOf[UnsafeRow]
  }

  def createUnsafeRow(i: Long): UnsafeRow = {
    val data = new Array[Byte](16)
    val row = new UnsafeRow
    row.pointTo(data, 1, 16)
    row.setLong(0, i)
    row
  }

  test("GeneralHashedRelation") {
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val numDataRows = SQLMetrics.createLongMetric(sparkContext, "data")
    val hashed = HashedRelation(data.iterator, numDataRows, keyProjection, data.size)
    assert(hashed.isInstanceOf[GeneralHashedRelation])

    assert(hashed.get(data(0)) === CompactBuffer[InternalRow](data(0)))
    assert(hashed.get(data(1)) === CompactBuffer[InternalRow](data(1)))
    assert(hashed.get(InternalRow(10)) === null)

    val data2 = CompactBuffer[InternalRow](data(2))
    data2 += data(2)
    assert(hashed.get(data(2)) === data2)
    assert(numDataRows.value.value === data.length)
  }

  test("UniqueKeyHashedRelation") {
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2))
    val numDataRows = SQLMetrics.createLongMetric(sparkContext, "data")
    val hashed = HashedRelation(data.iterator, numDataRows, keyProjection, data.size)
    assert(hashed.isInstanceOf[UniqueKeyHashedRelation])

    assert(hashed.get(data(0)) === CompactBuffer[InternalRow](data(0)))
    assert(hashed.get(data(1)) === CompactBuffer[InternalRow](data(1)))
    assert(hashed.get(data(2)) === CompactBuffer[InternalRow](data(2)))
    assert(hashed.get(InternalRow(10)) === null)

    val uniqHashed = hashed.asInstanceOf[UniqueKeyHashedRelation]
    assert(uniqHashed.getValue(data(0)) === data(0))
    assert(uniqHashed.getValue(data(1)) === data(1))
    assert(uniqHashed.getValue(data(2)) === data(2))
    assert(uniqHashed.getValue(InternalRow(10)) === null)
    assert(numDataRows.value.value === data.length)
  }

  test("FixedSizeHashRelation") {
    val data = Array(createUnsafeRow(0), createUnsafeRow(1), createUnsafeRow(2))
    val hashed = new FixedSizeHashRelation(data.toIterator, unsafeKeyProjection, data.size)
    assert(hashed.get(data(0)).size === 1)
    assert(hashed.get(data(0)).head === data(0))
    assert(hashed.get(data(1)).size === 1)
    assert(hashed.get(data(1)).head === data(1))
    assert(hashed.get(data(2)).size === 1)
    assert(hashed.get(data(2)).head === data(2))
    assert(hashed.get(InternalRow(10)) === null)

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    hashed.writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new FixedSizeHashRelation()
    hashed2.readExternal(in)
    assert(hashed2.get(data(0)).size === 1)
    assert(hashed2.get(data(0)).head === data(0))
    assert(hashed2.get(data(1)).size === 1)
    assert(hashed2.get(data(1)).head === data(1))
    assert(hashed2.get(data(2)).size === 1)
    assert(hashed2.get(data(2)).head === data(2))
    assert(hashed2.get(InternalRow(10)) === null)
  }

  test("UnsafeHashedRelation") {
    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val numDataRows = SQLMetrics.createLongMetric(sparkContext, "data")
    val toUnsafe = UnsafeProjection.create(schema)
    val unsafeData = data.map(toUnsafe(_).copy()).toArray

    val buildKey = Seq(BoundReference(0, IntegerType, false))
    val keyGenerator = UnsafeProjection.create(buildKey)
    val hashed = UnsafeHashedRelation(unsafeData.iterator, numDataRows, keyGenerator, 1)
    assert(hashed.isInstanceOf[UnsafeHashedRelation])

    assert(hashed.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed.get(toUnsafe(InternalRow(10))) === null)

    val data2 = CompactBuffer[InternalRow](unsafeData(2).copy())
    data2 += unsafeData(2).copy()
    assert(hashed.get(unsafeData(2)) === data2)

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    hashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)
    assert(hashed2.get(unsafeData(0)) === CompactBuffer[InternalRow](unsafeData(0)))
    assert(hashed2.get(unsafeData(1)) === CompactBuffer[InternalRow](unsafeData(1)))
    assert(hashed2.get(toUnsafe(InternalRow(10))) === null)
    assert(hashed2.get(unsafeData(2)) === data2)
    assert(numDataRows.value.value === data.length)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.asInstanceOf[UnsafeHashedRelation].writeExternal(out2)
    out2.flush()
    // This depends on that the order of items in BytesToBytesMap.iterator() is exactly the same
    // as they are inserted
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("test serialization empty hash map") {
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    val hashed = new UnsafeHashedRelation(
      new java.util.HashMap[UnsafeRow, CompactBuffer[UnsafeRow]])
    hashed.writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)

    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val toUnsafe = UnsafeProjection.create(schema)
    val row = toUnsafe(InternalRow(0))
    assert(hashed2.get(row) === null)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.writeExternal(out2)
    out2.flush()
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  def testHashedRelation(ht: HashedRelation, data: Array[InternalRow], iters: Int): (Long, Long) = {
    val start = System.currentTimeMillis()
    var matches = 0L
    for (i <- 1 to iters) {
      data.foreach { r =>
        val find = ht.get(r)
        if (find != null) matches += find.size
      }
    }
    (matches, System.currentTimeMillis() - start)
  }

  /*
BroadcastHashedRelation: 2571 30000000
  Rate:  11.67 M/sec
UnsafeRow Hash Table: 3461 30000000
  Rate:   8.67 M/sec
UnsafeRow BytesToBytesMap: 3878 30000000
  Rate:   7.74 M/sec
   */
  test("Measure hashed relation perf") {
    val N = 10000L
    val iters = 6000
    val buildData = mutable.ArrayBuffer.empty[UnsafeRow]
    val probeData = mutable.ArrayBuffer.empty[UnsafeRow]
    (1L to N).foreach { i => buildData += createUnsafeRow(i * 2) }
    (1L to 2 * N).foreach { i => probeData += createUnsafeRow(i) }

    val numDataRows = SQLMetrics.createLongMetric(sparkContext, "data")

    // Fixed size hash table for all internal rows.
    val broadcastHashedRelation = new FixedSizeHashRelation(
        buildData.toIterator, unsafeKeyProjection, buildData.size)

    // Unsafe row but running in local mode.
    val unsafeHashed = HashedRelation(
        buildData.iterator, numDataRows, unsafeKeyProjection, buildData.size)

    // Unsafe row using bytes to bytes map (similar to broadcast join).
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    unsafeHashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
    out.flush()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val unsafeBytesToBytes = new UnsafeHashedRelation()
    unsafeBytesToBytes.readExternal(in)

    // scalastyle:off
    testHashedRelation(broadcastHashedRelation, probeData.toArray, 1)
    val (matches1, time1) = testHashedRelation(broadcastHashedRelation, probeData.toArray, iters)
    println("BroadcastHashedRelation: " + time1 + " " + matches1)
    println("  Rate: " + ("%6.2f").format(matches1 * 2 / 1000. / time1) + " M/sec")

    testHashedRelation(unsafeHashed, probeData.toArray, 1)
    val (matches2, time2) = testHashedRelation(unsafeHashed, probeData.toArray, iters)
    println("UnsafeRow Hash Table: " + time2 + " " + matches2)
    println("  Rate: " + ("%6.2f").format(matches2 * 2 / 1000. / time2) + " M/sec")

    testHashedRelation(unsafeBytesToBytes, probeData.toArray, 1)
    val (matches3, time3) = testHashedRelation(unsafeBytesToBytes, probeData.toArray, iters)
    println("UnsafeRow BytesToBytesMap: " + time3 + " " + matches3)
    println("  Rate: " + ("%6.2f").format(matches3 * 2 / 1000. / time3) + " M/sec")
  }
}
