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

package com.haima.sage.bigdata.analyzer.ml

import java.nio.charset.Charset

import com.haima.sage.bigdata.analyzer.math.Murmur3_x86_32._
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{SparseVector, Vector}

import scala.collection.mutable

/**
  * Maps a sequence of terms to their term frequencies using the hashing trick.
  *
  * @param numFeatures   number of features (default: 2^20^)
  * @param binary        (default: false)  If true, term frequency vector will be binary such that non-zero term counts will be set to 1
  * @param hashAlgorithm (default: murmur3)     Set the hash algorithm used when mapping term to integer.
  */
class HashingTF(numFeatures: Int = 1 << 20,
                binary: Boolean = false,
                hashAlgorithm: String = HashingTF.Murmur3
               ) extends Serializable {

  import HashingTF._

  /**
    * Returns the index of the input term.
    */

  def indexOf(term: Any): Int = {
    nonNegativeMod(getHashFunction(term), numFeatures)
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /**
    * Get the hash function corresponding to the current [[hashAlgorithm]] setting.
    */
  private lazy val getHashFunction: Any => Int = hashAlgorithm match {
    case Murmur3 => murmur3Hash
    case Native => nativeHash
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  def empty: Vector = SparseVector.fromCOO(numFeatures, mutable.HashMap.empty[Int, Double])

  /**
    * Transforms the input document into a sparse term frequency vector.
    */

  def transform[D <: Iterable[_]](document: D): SparseVector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]

    def setTF(i: Int) =
      if (binary)
        1.0
      else termFrequencies.getOrElse(i, 0.0) + 1.0

    document.asInstanceOf[Iterable[_]].foreach { term =>
      val i = nonNegativeMod(getHashFunction(term), numFeatures)
      termFrequencies.put(i, setTF(i))
    }
    SparseVector.fromCOO(numFeatures, termFrequencies.toSeq)
  }


  /**
    * Transforms the input document to term frequency vectors.
    */

  def transform[D <: Iterable[_]](dataset: DataSet[D]): DataSet[Vector] = {
    dataset.map(t => transform[D](t))
  }

  //  /**
  //   * Transforms the input document to term frequency vectors (Java version).
  //   */
  //
  //  def transform[D <: JavaIterable[_]](dataset: DataSet[D]): DataSet[Vector] = {
  //
  //
  //    dataset.map(this.transform)
  //  }
}

object HashingTF {

  private[HashingTF] val Native: String = "native"

  private[HashingTF] val Murmur3: String = "murmur3"

  private val seed = 42

  /**
    * Calculate a hash code value for the term object using the native Scala implementation.
    * This is the default hash algorithm used in Spark 1.6 and earlier.
    */
  private[HashingTF] def nativeHash(term: Any): Int = term.##

  /**
    * Calculate a hash code value for the term object using
    * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
    * This is the default hash algorithm used from Spark 2.0 onwards.
    */
  def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>


        hashUnsafeBytes(s.getBytes(Charset.forName("UTF-8")), 0, s.length, seed)
      case s => hashUnsafeBytes(s.toString, 0, s.toString.length, seed)
    }
  }
}
