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

package org.apache.spark.sql

import java.io.File
import java.nio.ByteBuffer
import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.sql.CachedScanBenchmark.Benchmark.Case
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.RowBatch
import org.apache.spark.sql.execution.IntHashSet
import org.apache.spark.sql.execution.datasources.parquet.UnsafeRowParquetRecordReader
import org.apache.spark.sql.types.{StringType, IntegerType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Benchmark suite to evaluate the performance of reading from a cached table.
 */
object CachedScanBenchmark {
  val iters = 5
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)
  val STORE_SALES =  "/Users/nong/Data/tpcds_sf1/store_sales_flat_uncompressed"
  val ITEM = "/Users/nong/Data/tpcds_sf1/item"
  val storeSales = sqlContext.read.parquet(STORE_SALES)
  val item = sqlContext.read.parquet(ITEM)

  storeSales.registerTempTable("store_sales")
  item.registerTempTable("item")

  class Benchmark(name: String, num: Long, iters: Int = 5) {
    val benchmarks = mutable.ArrayBuffer.empty[Case]

    def add(name: String, f: Int => Unit): Unit = {
      benchmarks += Case(name, f)
    }

    def run(): Unit = {
      val results = benchmarks.map { c =>
        Benchmark.measure(c.name, num, c.fn, iters)
      }
      // scalastyle:off
      printf("%-24s %14s %14s\n","Benchmark: " + name, "Avg Time(ms)", "Avg Rate(M/s)")
      println("-----------------------------------------------------------")
      results.zip(benchmarks).foreach { r =>
        printf("%-24s %14s %14s\n", r._2.name, r._1.avgMs.toString, "%10.2f" format r._1.avgRate)
      }
      // scalastyle:on
    }
  }

  object Benchmark {
    case class Case(name: String, fn: Int => Unit)
    case class Result(avgMs: Double, avgRate: Double)

    def measure(name: String, num: Long, f: Int => Unit, iters: Int): Result = {
      var totalTime = 0L
      for (i <- 0 until iters + 1) {
        val start = System.currentTimeMillis()

        f(i)

        val end = System.currentTimeMillis()
        if (i != 0) totalTime += end - start
      }
      Result(totalTime.toDouble / iters, num * iters / totalTime.toDouble / 1000)
    }
  }

  def getListOfPaths(path: String): List[String] = {
    val d = new File(path)
    if (d.exists) {
      if (d.isDirectory) {
        d.listFiles.filter { p => p.getName.charAt(0) != '.' }.map(_.getAbsolutePath)
          .flatMap(getListOfPaths(_)).toList
      } else {
        path :: Nil
      }
    } else {
      List[String]()
    }
  }

  def memoryAccess(): Unit = {
    val count = 32 * 1000
    val iters = 10000L

    val javaArray = { i: Int =>
      val data = new Array[Int](count)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data(i) = i
          i += 1
        }
        i = 0
        while (i < count) {
          sum += data(i)
          i += 1
        }
      }
      println(sum)
    }


    val byteBufferUnsafe = { i: Int =>
      val data = ByteBuffer.allocate(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          Platform.putInt(data.array(), Platform.BYTE_ARRAY_OFFSET + i * 4, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += Platform.getInt(data.array(), Platform.BYTE_ARRAY_OFFSET + i * 4)
          i += 1
        }
      }
      println(sum)
    }

    val directByteBuffer = { i: Int =>
      val data = ByteBuffer.allocateDirect(count * 4).asIntBuffer()
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data.put(i)
          i += 1
        }
        data.rewind()
        i = 0
        while (i < count) {
          sum += data.get()
          i += 1
        }
        data.rewind()
      }
      println(sum)
    }

    val byteBufferApi = { i: Int =>
      val data = ByteBuffer.allocate(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          data.putInt(i)
          i += 1
        }
        data.rewind()
        i = 0
        while (i < count) {
          sum += data.getInt()
          i += 1
        }
        data.rewind()
      }
      println(sum)
    }

    val unsafeBuffer = { i: Int =>
      val data: Long = Platform.allocateMemory(count * 4)
      var sum = 0L
      for (n <- 0L until iters) {
        var ptr = data
        var i = 0
        while (i < count) {
          Platform.putInt(null, ptr, i)
          ptr += 4
          i += 1
        }
        ptr = data
        i = 0
        while (i < count) {
          sum += Platform.getInt(null, ptr)
          ptr += 4
          i += 1
        }
      }
      println(sum)
    }

    val rowBlock = { i: Int =>
      val column = RowBatch.Column.allocate(count, IntegerType, false)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          column.putInt(i, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += column.getInt(i)
          i += 1
        }
      }
      println(sum)
    }

    val rowBlockOffheap = { i: Int =>
      val column = RowBatch.Column.allocate(count, IntegerType, true)
      var sum = 0L
      for (n <- 0L until iters) {
        var i = 0
        while (i < count) {
          column.putInt(i, i)
          i += 1
        }
        i = 0
        while (i < count) {
          sum += column.getInt(i)
          i += 1
        }
      }
      println(sum)
    }

    /*
    Benchmark: Memory Access   Avg Time(ms)  Avg Rate(M/s)
    -----------------------------------------------------------
    Java Array                        199.2        1606.43
    ByteBuffer Unsaf                 347.4         921.13
    ByteBuffer API                   1657.8         193.03
    DirectByteBuffer                  645.2         495.97
    Unsafe Buffer                     193.4        1654.60
    Row Block                         209.8        1525.26
    Row Block Off Heap                349.8         914.81
     */
    val benchmark = new Benchmark("Memory Access", count * iters)
    benchmark.add("Java Array", javaArray)
    benchmark.add("ByteBuffer Unsafe", byteBufferUnsafe)
    benchmark.add("ByteBuffer API", byteBufferApi)
    benchmark.add("DirectByteBuffer", directByteBuffer)
    benchmark.add("Unsafe Buffer", unsafeBuffer)
    benchmark.add("Row Block", rowBlock)
    benchmark.add("Row Block Off Heap", rowBlockOffheap)
    benchmark.run()
  }


  def byteBufferAccess(): Unit = {
    val column = RowBatch.Column.allocate(1024, StringType, false)
    column.putUTF8(0, UTF8String.fromString("Hello"))
    column.putUTF8(1, UTF8String.fromString("abcdefg"))

    val v = new UTF8String()
    column.getUTF8(0, v)
    println(v.toString)
    column.getUTF8(1, v)
    println(v.toString)
  }

  @native def nativeScan(nulls: Long, values: Long, n: Int): Long

  def scan() : Unit = {
    val num = storeSales.count()
    val projection = "ss_item_sk" :: "ss_promo_sk" :: Nil

    val sqlQuery = { i: Int => {
      val agg = projection.map("sum(" + _ + ")")
      val sql = s"""select ${agg.mkString(", ")} from store_sales"""
      val result = sqlContext.sql(sql).take(1).head
      println("      result: " + result)
    }}

    val recordReader = { i: Int => {
      var sum1 = 0L
      var sum2 = 0L
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), false)
        while (reader.nextKeyValue()) {
          val record = reader.getCurrentValue
          if (!record.isNullAt(0)) sum1 += record.getInt(0)
          if (!record.isNullAt(1)) sum2 += record.getInt(1)
        }
        reader.close()
      }
      println("      result: " + sum1 + " " + sum2)
    }}

    def recordReaderRowBlock(offHeap: Boolean) = { i: Int => {
      var sum1 = 0L
      var sum2 = 0L
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), offHeap)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          var i = 0
          val numRows = storeSales.numRows()
          while (i < numRows) {
            if (!storeSales.getIsNull(0, i)) sum1 += storeSales.getInt(0, i)
            if (!storeSales.getIsNull(1, i)) sum2 += storeSales.getInt(1, i)
            i += 1
          }
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + sum1 + " " + sum2)
    }}

    val native = { i: Int => {
      var sum1 = 0L
      var sum2 = 0L
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), true)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          val numRows = storeSales.numRows()
          sum1 += nativeScan(storeSales.column(0).nullsNativeAddress(),
              storeSales.column(0).valuesNativeAddress, numRows)
          sum2 += nativeScan(storeSales.column(1).nullsNativeAddress(),
              storeSales.column(1).valuesNativeAddress, numRows)
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + sum1 + " " + sum2)
    }}

    /*
      Benchmark: Scan            Avg Time(ms)  Avg Rate(M/s)
      -----------------------------------------------------------
      SqlQuery                         3931.0           7.00
      RecordReader                     1579.4          17.42
      RowBatch                          442.2          62.21
      RowBatch OffHeap                  600.4          45.82
      Native                            424.2          64.85
     */
    val benchmark = new Benchmark("Scan", num)
    benchmark.add("SqlQuery", sqlQuery)
    benchmark.add("RecordReader", recordReader)
    benchmark.add("RowBatch", recordReaderRowBlock(false))
    benchmark.add("RowBatch OffHeap", recordReaderRowBlock(true))
    benchmark.add("Native", native)
    benchmark.run()
  }

  @native def nativeFilter(nulls: Long, values: Long, n: Int): Long

  def filter() : Unit = {
    val num = storeSales.count()
    val projection = "ss_item_sk" :: Nil

    val sqlQuery = { i: Int =>
      val sql = "select count(*) from store_sales where abs(ss_item_sk) = 1000"
      println("      result: " + sqlContext.sql(sql).take(1).head.getLong(0))
    }

    val recordReader = { i: Int =>
      var count = 0
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), false)
        while (reader.nextKeyValue()) {
          val record = reader.getCurrentValue
          if (!record.isNullAt(0)) {
            val v = record.getInt(0)
            if (Math.abs(v) == 1000) count += 1
          }
        }
        reader.close()
      }
      println("      result: " + count)
    }

    def recordReaderRowBlock(offHeap: Boolean) = { i: Int => {
      var count = 0
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), offHeap)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          var i = 0
          val numRows = storeSales.numRows()
          while (i < numRows) {
            if (!storeSales.getIsNull(0, i)) {
              val v = storeSales.getInt(0, i)
              if (Math.abs(v) == 1000) count += 1
            }
            i += 1
          }
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + count)
    }}

    val native = { i: Int => {
      var count = 0L
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), true)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          val numRows = storeSales.numRows()
          count += nativeFilter(storeSales.column(0).nullsNativeAddress(),
            storeSales.column(0).valuesNativeAddress, numRows)
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + count)
    }}

    /*
      Benchmark: Filter          Avg Time(ms)  Avg Rate(M/s)
      -----------------------------------------------------------
      SqlQuery                         1972.4          13.95
      RecordReader                      653.2          42.11
      RowBatch                          202.2         136.05
      RowBatch OffHeap                  220.2         124.92
      Native                            209.2         131.49
     */
    val benchmark = new Benchmark("Filter", num)
    benchmark.add("SqlQuery", sqlQuery)
    benchmark.add("RecordReader", recordReader)
    benchmark.add("RowBatch", recordReaderRowBlock(false))
    benchmark.add("RowBatch OffHeap", recordReaderRowBlock(true))
    benchmark.add("Native", native)
    benchmark.run
  }


  @native def nativeJoinBuild(nulls: Long, values: Long, count: Int): Long
  @native def nativeJoinProbe(nulls: Long, values: Long, count: Int): Long

  def joins() : Unit = {
    val num = storeSales.count()
    val projection = "ss_item_sk" :: Nil
    val itemCount: Long = item.count()

    val sqlQuery = { i: Int =>
      val sql = "select count(*) from store_sales join item where ss_item_sk = i_item_sk"
      println("      result: " + sqlContext.sql(sql).take(1).head.getLong(0))
    }

    val recordReader = { i: Int =>
      val table = new JavaHashMap[InternalRow, InternalRow](itemCount.toInt)

      getListOfPaths(ITEM).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("i_item_sk" :: Nil), false)
        while (reader.nextKeyValue()) {
          val record = reader.getCurrentValue.copy
          if (!record.anyNull()) table.put(record, record)
        }
        reader.close()
      }
      var count = 0
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), false)
        while (reader.nextKeyValue()) {
          val record = reader.getCurrentValue
          if (!record.isNullAt(0) && table.contains(record)) count += 1
        }
        reader.close()
      }
      println("      result: " + count)
    }

    def recordReaderRowBlock(offHeap: Boolean) = { i: Int =>
      val table = new IntHashSet(itemCount.toInt)

      getListOfPaths(ITEM).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("i_item_sk" :: Nil), offHeap)
        val item = reader.getRows
        while (item.numRows() > 0) {
          var i = 0
          val numRows = item.numRows()
          while (i < numRows) {
            if (!item.getIsNull(0, i)) {
              table.put(item.getInt(0, i))
            }
            i += 1
          }
          reader.getRows
        }
        reader.close()
      }

      var count = 0
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), offHeap)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          var i = 0
          val numRows = storeSales.numRows()
          while (i < numRows) {
            if (!storeSales.getIsNull(0, i)) {
              if (table.contains(storeSales.getInt(0, i))) {
                count += 1
              }
            }
            i += 1
          }
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + count)
    }

    val native = { i: Int =>
      getListOfPaths(ITEM).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("i_item_sk" :: Nil), true)
        val item = reader.getRows
        while (item.numRows() > 0) {
          nativeJoinBuild(item.column(0).nullsNativeAddress(),
            item.column(0).valuesNativeAddress(), item.numRows())
          reader.getRows
        }
        reader.close()
      }

      var count = 0L
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList(projection), true)
        val storeSales = reader.getRows
        while (storeSales.numRows() > 0) {
          count += nativeJoinProbe(storeSales.column(0).nullsNativeAddress(),
            storeSales.column(0).valuesNativeAddress(), storeSales.numRows())
          reader.getRows
        }
        reader.close()
      }
      println("      result: " + count)
    }

    /*
    Benchmark: Join            Avg Time(ms)  Avg Rate(M/s)
    -----------------------------------------------------------
    SqlQuery                         5901.8           4.66
    RecordReader                     2030.0          13.55
    RowBatch                          253.4         108.56
    RowBatch Off Heap                 265.8         103.49
    Native                            246.8         111.46
    */
    val benchmark = new Benchmark("Join", num)
    benchmark.add("SqlQuery", sqlQuery)
    benchmark.add("RecordReader", recordReader)
    benchmark.add("RowBatch", recordReaderRowBlock(false))
    benchmark.add("RowBatch Off Heap", recordReaderRowBlock(true))
    benchmark.add("Native", native)
    benchmark.run
  }

  def q19() : Unit = {
    val dateDimPath = "/Users/nong/Data/tpcds_sf1/date_dim"
    val customerPath = "/Users/nong/Data/tpcds_sf1/customer"
    val customerAddressPath = "/Users/nong/Data/tpcds_sf1/customer_address"
    val storePath = "/Users/nong/Data/tpcds_sf1/store"

    sqlContext.read.parquet(dateDimPath).registerTempTable("date_dim")
    sqlContext.read.parquet(customerPath).registerTempTable("customer")
    sqlContext.read.parquet(customerAddressPath).registerTempTable("customer_address")
    sqlContext.read.parquet(storePath).registerTempTable("store")


    val num = storeSales.count()

    val sqlQuery = { i: Int =>
        val sql =
          s"""
           | select i_brand_id brand_id, sum(ss_ext_sales_price) ext_price, count(*) from
           |   store_sales
           |   join item on (store_sales.ss_item_sk = item.i_item_sk)
           |   join customer on (store_sales.ss_customer_sk = customer.c_customer_sk)
           |   join customer_address on (customer.c_current_addr_sk = customer_address.ca_address_sk)
           |   join store on (store_sales.ss_store_sk = store.s_store_sk)
           |   join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
           | where i_manager_id = 8
           |    and d_moy = 11
           |    and d_year = 1998
           |    and i_brand_id = 2002001
           |  group by i_brand_id
         """.stripMargin
        println("      result: " + sqlContext.sql(sql).take(1).head)
      }


    def recordReaderRowBlock(offHeap: Boolean) = { i: Int =>
      case class Val(var result: Int = 0)

      val itemHt = new JavaHashMap[Integer, UnsafeRow]()
      val dateDimHt = new JavaHashMap[Integer, UnsafeRow]()
      val customerSkHt = new JavaHashMap[Integer, UnsafeRow]()
      val customerAddrHt = new JavaHashMap[Integer, UnsafeRow]()
      val storeHt = new JavaHashMap[Integer, UnsafeRow]()

      val aggregation = new JavaHashMap[Integer, Val]()
      var matches = 0

      // Build item ht
      getListOfPaths(ITEM).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(
            p, seqAsJavaList("i_manager_id" :: "i_item_sk" :: "i_brand_id" :: Nil), offHeap)
        while (reader.nextKeyValue()) {
          val record: UnsafeRow = reader.getCurrentValue
          if (!record.isNullAt(0) && record.getInt(0) == 8) {
            if (!record.isNullAt(2) && record.getInt(2) == 2002001) {
              if (!record.isNullAt(1)) itemHt.put(record.getInt(1), record.copy)
            }
          }
        }
        reader.close()
      }

      // Build customer table
      getListOfPaths(customerPath).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("c_customer_sk" :: "c_current_addr_sk" :: Nil), offHeap)
        while (reader.nextKeyValue()) {
          val record: UnsafeRow = reader.getCurrentValue
          if (!record.anyNull()) customerSkHt.put(record.getInt(0), record.copy)
        }
        reader.close()
      }

      // Build customer address table
      getListOfPaths(customerAddressPath).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("ca_address_sk" :: Nil), false)
        while (reader.nextKeyValue()) {
          val record: UnsafeRow = reader.getCurrentValue
          if (!record.isNullAt(0)) customerAddrHt.put(record.getInt(0), record.copy)
        }
        reader.close()
      }


      // Read store, apply predicates and build hash table.
      getListOfPaths(storePath).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("s_store_sk" :: Nil), offHeap)
        while (reader.nextKeyValue()) {
          val record: UnsafeRow = reader.getCurrentValue
          if (!record.isNullAt(0)) storeHt.put(record.getInt(0), record.copy)
        }
        reader.close()
      }

      // Read date dim, apply predicates and build hash table.
      getListOfPaths(dateDimPath).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("d_date_sk" :: "d_moy" :: "d_year" :: Nil), offHeap)
        while (reader.nextKeyValue()) {
          val record: UnsafeRow = reader.getCurrentValue
          if (!record.anyNull()) {
            if (record.getInt(1) == 11 && record.getInt(2) == 1998) {
              dateDimHt.put(record.getInt(0), record.copy)
            }
          }
        }
        reader.close()
      }

      // Read store_sales, do the joins and agg
      getListOfPaths(STORE_SALES).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, seqAsJavaList("ss_item_sk" :: "ss_customer_sk" ::
            "ss_store_sk" :: "ss_sold_date_sk" :: "ss_ext_sales_price" :: Nil), offHeap)
        val storeSales = reader.getRows
        val ss_item_sk = storeSales.column(0)
        val ss_customer_sk = storeSales.column(1)
        val ss_store_sk = storeSales.column(2)
        val ss_sold_date_sk = storeSales.column(3)
        val ss_ext_sales_price = storeSales.column(4)

        while (storeSales.numRows() > 0) {
          var i = 0
          val numRows = storeSales.numRows()
          while (i < numRows) {
            // Join with item
            if (!ss_item_sk.getIsNull(i) && itemHt.contains(ss_item_sk.getInt(i))) {
              if (!ss_customer_sk.getIsNull(i) &&
                  customerSkHt.containsKey(ss_customer_sk.getInt(i))) {
                val customerRecord = customerSkHt.get(ss_customer_sk.getInt(i))
                val c_current_addr_sk = customerRecord.getInt(1)
                // Join with customer_address
                if (customerAddrHt.contains(c_current_addr_sk)) {
                  // Join with store
                  if (storeHt.contains(ss_store_sk.getInt(i))) {
                    // Join with date dim
                    if (dateDimHt.contains(ss_sold_date_sk.getInt(i))) {
                      if (!ss_ext_sales_price.getIsNull(i)) {
                        matches += 1
                        val itemRecord = itemHt.get(ss_item_sk.getInt(i))
                        val brand = itemRecord.getInt(2)
                        if (aggregation.contains(brand)) {
                          aggregation.get(brand).result += ss_ext_sales_price.getInt(i)
                        } else {
                          aggregation.put(brand, new Val(ss_ext_sales_price.getInt(i)))
                        }
                      }
                    }
                  }
                }
              }
            }
            i += 1
          }
          reader.getRows
        }
        reader.close()
      }

      println(aggregation.toString + " " + matches)
    }

    /*
    Benchmark: Q19             Avg Time(ms)  Avg Rate(M/s)
    -----------------------------------------------------------
    SqlQuery                         9302.4           2.96
    RowBatch                         1990.0          13.82
    RowBatch Off Heap                2146.0          12.82
    */
    val benchmark = new Benchmark("Q19", num)
    //benchmark.add("SqlQuery", sqlQuery)
    benchmark.add("RowBatch", recordReaderRowBlock(false))
    //benchmark.add("RowBatch Off Heap", recordReaderRowBlock(true))
    benchmark.run
  }

  def main(args: Array[String]): Unit = {
    System.load("/Users/nong/Projects/jni/benchmark.dylib")

    //memoryAccess()
    //byteBufferAccess()
    //scan()
    //filter()
    //joins()
    q19()
  }
}
