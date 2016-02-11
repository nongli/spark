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
package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure parquet read performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object ParquetReadBenchmark {
  val conf = new SparkConf()
  conf.set("spark.sql.parquet.compression.codec", "snappy")
  conf.set("spark.sql.shuffle.partitions", "4")
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(sqlContext.dropTempTable)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => sqlContext.conf.setConfString(key, value)
        case (key, None) => sqlContext.conf.unsetConf(key)
      }
    }
  }

  def intScanBenchmark(values: Int): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark = new Benchmark("SQL Single Int Column Scan", values)
    // Benchmarks driving reader component directly.
    val parquetReaderBenchmark = new Benchmark("Parquet Reader Single Int Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        sqlContext.range(values).registerTempTable("t1")
        sqlContext.sql("select cast(id as INT) as id from t1")
            .write.parquet(dir.getCanonicalPath)
        sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("tempTable")

        sqlBenchmark.addCase("SQL Parquet Reader") { iter =>
          sqlContext.sql("select sum(id) from tempTable").collect()
        }

        sqlBenchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key -> "false") {
            sqlContext.sql("select sum(id) from tempTable").collect()
          }
        }

        sqlBenchmark.addCase("SQL Parquet Vectorized") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            sqlContext.sql("select sum(id) from tempTable").collect()
          }
        }

        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        // Driving the parquet reader directly without Spark.
        parquetReaderBenchmark.addCase("ParquetReader") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            reader.initialize(p, ("id" :: Nil).asJava)
            reader.resultBatch

            while (reader.nextKeyValue()) {
              val record = reader.getCurrentValue
              if (!record.isNullAt(0)) sum += record.getInt(0)
            }
            println(sum)
            reader.close()
          }
        }

        // Driving the parquet reader in batch mode directly.
        parquetReaderBenchmark.addCase("ParquetReader(Batched)") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val col = batch.column(0)
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!col.getIsNull(i)) sum += col.getInt(i)
                  i += 1
                }
              }
              println(sum)
            } finally {
              reader.close()
            }
          }
        }

        // Decoding in vectorized but having the reader return rows.
        parquetReaderBenchmark.addCase("ParquetReader(Batch -> Row)") { num =>
          var sum = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              while (reader.nextBatch()) {
                val it = batch.rowIterator()
                while (it.hasNext) {
                  val record = it.next()
                  if (!record.isNullAt(0)) sum += record.getInt(0)
                }
              }
              println(sum)
            } finally {
              reader.close()
            }
          }
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        SQL Single Int Column Scan:        Avg Time(ms)    Avg Rate(M/s)  Relative Rate
        -------------------------------------------------------------------------------
        SQL Parquet Reader                      1350.56            11.65         1.00 X
        SQL Parquet MR                          1844.09             8.53         0.73 X
        SQL Parquet Vectorized                  1062.04            14.81         1.27 X
        */
        //sqlBenchmark.run()

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Parquet Reader Single Int Column Scan:     Avg Time(ms)    Avg Rate(M/s)  Relative Rate
        -------------------------------------------------------------------------------
        ParquetReader                            610.40            25.77         1.00 X
        ParquetReader(Batched)                   172.66            91.10         3.54 X
        ParquetReader(Batch -> Row)              192.28            81.80         3.17 X
        */
        parquetReaderBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        sqlContext.range(values).registerTempTable("t1")
        sqlContext.sql("select cast(id as INT) as c1, cast(id as STRING) as c2 from t1")
            .write.parquet(dir.getCanonicalPath)
        sqlContext.read.parquet(dir.getCanonicalPath).registerTempTable("tempTable")

        val benchmark = new Benchmark("Int and String Scan", values)

        benchmark.addCase("SQL Parquet Reader") { iter =>
          sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
        }

        benchmark.addCase("SQL Parquet MR") { iter =>
          withSQLConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key -> "false") {
            sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
          }
        }

        benchmark.addCase("SQL Parquet Vectorized") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            sqlContext.sql("select sum(c1), sum(length(c2)) from tempTable").collect
          }
        }


        val files = SpecificParquetRecordReaderBase.listDirectory(dir).toArray
        benchmark.addCase("ParquetReader") { num =>
          var sum1 = 0L
          var sum2 = 0L
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new UnsafeRowParquetRecordReader
            reader.initialize(p, null)
            while (reader.nextKeyValue()) {
              val record = reader.getCurrentValue
              if (!record.isNullAt(0)) sum1 += record.getInt(0)
              if (!record.isNullAt(1)) sum2 += record.getUTF8String(1).numBytes()
            }
            reader.close()
          }
        }

        /*
        Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
        Int and String Scan:               Avg Time(ms)    Avg Rate(M/s)  Relative Rate
        -------------------------------------------------------------------------------
        SQL Parquet Reader                      1737.94             6.03         1.00 X
        SQL Parquet MR                          2393.08             4.38         0.73 X
        SQL Parquet Vectorized                  1442.99             7.27         1.20 X
        ParquetReader                           1032.11            10.16         1.68 X
        */
        benchmark.run()
      }
    }
  }

  val tpcds = Seq(
    ("q19", """
              |-- start query 1 in stream 0 using template query19.tpl
              |select
              |  i_brand_id,
              |  i_brand,
              |  i_manufact_id,
              |  i_manufact,
              |  sum(ss_ext_sales_price) ext_price
              |from
              |  store_sales
              |  join item on (store_sales.ss_item_sk = item.i_item_sk)
              |  join customer on (store_sales.ss_customer_sk = customer.c_customer_sk)
              |  join customer_address on (customer.c_current_addr_sk = customer_address.ca_address_sk)
              |  join store on (store_sales.ss_store_sk = store.s_store_sk)
              |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
              |where
              |  --ss_date between '1999-11-01' and '1999-11-30'
              |  ss_sold_date_sk between 2451484 and 2451513
              |  and d_moy = 11
              |  and d_year = 1999
              |  and i_manager_id = 7
              |  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
              |group by
              |  i_brand,
              |  i_brand_id,
              |  i_manufact_id,
              |  i_manufact
              |order by
              |  ext_price desc,
              |  i_brand,
              |  i_brand_id,
              |  i_manufact_id,
              |  i_manufact
              |limit 100
              |-- end query 1 in stream 0 using template query19.tpl
            """.stripMargin) :: Nil).toArray


  def tpcdsBenchmark(): Unit = {
    val HOME = "/Users/nong/Data/tpcds-sf10/"
    val dir = HOME + "store_sales_snappy"
    sqlContext.read.parquet(HOME + "customer").registerTempTable("customer")
    sqlContext.read.parquet(HOME + "customer_address").registerTempTable("customer_address")
    sqlContext.read.parquet(HOME + "date_dim").registerTempTable("date_dim")
    sqlContext.read.parquet(HOME + "item").registerTempTable("item")
    sqlContext.read.parquet(HOME + "store").registerTempTable("store")
    //sqlContext.read.parquet(HOME + "store_sales").registerTempTable("store_sales")
    sqlContext.read.parquet(dir).registerTempTable("store_sales")

    sqlContext.conf.setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    val benchmark = new Benchmark("TPCDS", 28800501)
    val query = sqlContext.sql(tpcds(0).head._2)

    val files = SpecificParquetRecordReaderBase.listDirectory(new File(dir)).toArray
    // Driving the parquet reader directly without Spark.
    benchmark.addCase("ParquetReader") { num =>
      var sum = 0L
      files.map(_.asInstanceOf[String]).foreach { p =>
        val reader = new UnsafeRowParquetRecordReader
        reader.initialize(p, ("ss_store_sk" :: "ss_sold_date_sk" :: "ss_ext_sales_price"
          :: "ss_customer_sk" :: "ss_item_sk" :: Nil).asJava)
        val batch = reader.resultBatch()
        while (reader.nextBatch()) {
          val it = batch.rowIterator()
          while (it.hasNext) {
            val record = it.next()
            if (!record.isNullAt(0)) sum += 1
            if (!record.isNullAt(1)) sum += 1
            if (!record.isNullAt(2)) sum += 1
            if (!record.isNullAt(3)) sum += 1
            if (!record.isNullAt(4)) sum += 1
          }
        }
        println(sum)
        reader.close()
      }
    }

    //query.show
    //sqlContext.sql("select count(ss_store_sk), count(ss_sold_date_sk), count(ss_ext_sales_price), count(ss_customer_sk), count(ss_item_sk) from store_sales").show
    //sqlContext.sql("select count(ss_item_sk) from store_sales").show

    benchmark.addCase("Q19") { i =>
      //sqlContext.sql("select count(ss_ext_sales_price) from store_sales").show
      //sqlContext.sql("select count(ss_store_sk), count(ss_sold_date_sk), count(ss_ext_sales_price), count(ss_customer_sk), count(ss_item_sk) from store_sales").show
      //sqlContext.sql("select count(ss_store_sk), count(ss_sold_date_sk), count(ss_customer_sk), count(ss_item_sk) from store_sales").show
      query.show
    }
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    //intScanBenchmark(1024 * 1024 * 15)
    //intStringScanBenchmark(1024 * 1024 * 10)
    tpcdsBenchmark()
  }
}
