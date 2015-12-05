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

package org.apache.spark.sql.execution.joins;

import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.util.collection.CompactBuffer;
import sun.misc.Unsafe;

/**
 * This is a simple in memory hash table that does not grow. This should be used when the
 * input size is known (so the hash table can be presized).
 */
public class FixedSizeHashTable {
  /**
   * Creates a hash table to hold `size` rows.
   */
  public FixedSizeHashTable(int size) throws SparkException {
    int numBuckets = (int) ByteArrayMethods.nextPowerOf2(size * 2);
    if (numBuckets < 0) {
      throw new SparkException(
          "FixedSizeHashTable cannot be used for such a large number of rows: " + size);
    }
    buckets = new Node[numBuckets];
    numBucketsMinus1 = numBuckets - 1;
  }

  /**
   * Node of hash table. Contains the key/value and the chaining structure.
   */
  public final static class Node {
    // Cache of key.hashNode
    private final int hash;
    private final UnsafeRow key;

    // Rows with this key. This stores all rows with the same key.
    private CompactBuffer<UnsafeRow> rows;

    // Chain to next row. null if end of the hash chain. This is for nodes with hash collisions
    private Node next;

    public CompactBuffer<UnsafeRow> getRows() { return rows; }
    public void setRows(CompactBuffer<UnsafeRow> rows) { this.rows = rows; }

    private Node(UnsafeRow key, int hash, Node next) {
      this.key = key;
      this.rows = null;
      this.hash = hash;
      this.next = next;
    }
  }

  /**
   * Finds rows that match `key`. Returns an iterator to find all matches.
   */
  public Node findToInsert(UnsafeRow key) {
    int hash = key.hashCode();
    int bucket = hash & numBucketsMinus1;
    Node node = findMatch(buckets[bucket], key, hash);
    if (node == null) {
      node = new Node(key, hash, buckets[bucket]);
      buckets[bucket] = node;
      return node;
    } else {
      return node;
    }
  }

  /**
   * Finds all the rows that match `key`. The results are stored in r2. If there are no
   * matches, returns null, otherwise returns `results`.
   */
  public CompactBuffer<UnsafeRow> find(UnsafeRow key) {
    int hash = key.hashCode();
    int bucket = hash & numBucketsMinus1;
    Node node = findMatch(buckets[bucket], key, hash);
    if (node == null) return null;
    return node.getRows();
  }

  /**
   * Walks the linked list for node until the next match. Returns null if there is no match.
   */
  private Node findMatch(Node node, UnsafeRow key, int hash) {
    while (node != null) {
      if (node.hash == hash && node.key.equals(key)) break;
      node = node.next;
    }
    return node;
  }

  /**
   * Array of buckets that is the hash table.
   */
  private final Node[] buckets;

  /**
   * Cache of buckets.size - 1. The size of buckets is always a power of 2.
   */
  private final int numBucketsMinus1;
}
