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
package org.apache.spark.util

import scala.collection.mutable

class EventTimeLine(name: String) {
  case class Event(val name: String, timeMs: Long)

  val startTimeMs = System.currentTimeMillis()
  val events = mutable.ArrayBuffer.empty[Event]

  def addEvent(name: String): Unit = {
    events += new Event(name, System.currentTimeMillis())
  }

  def eventString(): String = {
    val total = if (events.length > 0) {
      events.last.timeMs - startTimeMs
    } else {
      0
    }
    val sb = new StringBuilder()
    sb.append(name + "(" + Utils.msDurationToString(total) + ")\n")
    var lastTime = startTimeMs
    events.foreach { e =>
      sb.append("  " + e.name + ": " + Utils.msDurationToString(e.timeMs - lastTime) + "\n")
      lastTime = e.timeMs
    }
    sb.toString()
  }
}
