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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._


class GenerateProfilingProjection(name: String) extends CodeGenerator[Seq[Expression], Projection] {

  def generate(): Projection = {
    create(Seq.empty[Expression])
  }

  override protected def create(in: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val className = s"${name}_ProfilingProjectionExecution"

    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new $className();
      }

      class $className extends ${classOf[BaseProjection].getName} {
        // Scala.Function1 need this
        public java.lang.Object apply(java.lang.Object row) {
          return row;
        }

        public InternalRow apply(InternalRow ${ctx.INPUT_ROW}) {
          return ${ctx.INPUT_ROW};
        }
      }
      """

    logDebug(s"code for $in:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[Projection]
  }

  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = {
    in
  }

  /** Binds an input expression to a given input schema */
  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = {
    in.map(BindReferences.bindReference(_, inputSchema))
  }
}
