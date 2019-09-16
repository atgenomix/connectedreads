/**
  * Copyright (C) 2015, Atgenomix Incorporated. All Rights Reserved.
  * This program is an unpublished copyrighted work which is proprietary to
  * Atgenomix Incorporated and contains confidential information that is not to
  * be reproduced or disclosed to any other person or entity without prior
  * written consent from Atgenomix, Inc. in each and every instance.
  * Unauthorized reproduction of this program as well as unauthorized
  * preparation of derivative works based upon the program or distribution of
  * copies by sale, rental, lease or lending are violations of federal copyright
  * laws and state trade secret laws, punishable by civil and criminal penalties.
  */

package com.atgenomix.connectedreads.core.rdd

import com.atgenomix.connectedreads.io.FileFormat
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class Fragments(@transient spark: SparkSession, fragments: DataFrame) {

  def toPairedSegments(ff: FileFormat.Value, minsfx: Int, querylength: java.lang.Integer): Dataset[(String, Long)] = {
    import FileFormat._
    import spark.implicits._

    val encoded = ff match {
      case FASTA =>
        fragments.mapPartitions(df => SegmentFactory[Segmenter](ff)(df, querylength).segment)
          // KeyValueGroupedDataset.reduceGroups should support partial aggregation
          // https://issues.apache.org/jira/browse/SPARK-16391
          .groupByKey(_._1)
          .reduceGroups((x, y) => (x._1, combine(x._2, y._2)))
          .map(_._2.swap)
      case FASTQ =>
        fragments.map(x => (x.getAs[String](0).split(" ", 2)(0), x.getAs[String](1)))
          .groupByKey(_._1)
          .reduceGroups((x, y) => (x._1, x._2 + "^" + y._2))
          .map(_._2._2) // left the joined parts and drop others
          .repartition(1000)
          .mapPartitions(iter => {
            // paired-sequence by part of readname
            val buf = new ArrayBuffer[(String, Long)]()
            var sn = TaskContext.getPartitionId() * 1000000
            while (iter.hasNext) {
              iter.next().split("\\^").map(x => buf += ((x, sn)))
              sn = sn + 1
            }
            buf.iterator
          })
    }

    // (String, Long) => (Seq, Pos)
    encoded.mapPartitions(normalizeSplit(_, minsfx))
  }

  /**
    * Normalize heading and tailing N, and split fragment by N so that the resulting
    * string does not contain any N.
    *
    * @param iter   list of input string and its position
    * @param minsfx the minimal suffix length
    * @return list of normalized string and adjusted position
    */
  private def normalizeSplit(iter: Iterator[(String, Long)], minsfx: Int): Iterator[(String, Long)] = {
    val buf = new ArrayBuffer[(String, Long)]()

    def _split(s: String, b: Int, e: Int, p: Long): Unit = {
      var bos = b
      var eos = bos + 1
      while (eos < e) {
        if (s.charAt(eos) == 'N') {
          if (eos - bos >= minsfx) buf += ((s.substring(bos, eos), p + bos))
          bos = eos + 1
          while (bos < e && s.charAt(bos) == 'N') bos += 1
          eos = bos
        } else {
          eos += 1
          if (eos >= e && eos - bos >= minsfx) {
            if (eos - bos == s.length)
              buf += ((s, p))
            else
              buf += ((s.substring(bos, eos), p + bos))
          }
        }
      }
    }

    while (iter.hasNext) {
      val x = iter.next
      // remaining isolated entities like (-ACTG, 1) => (ACTG, 1)
      val str = if (x._1.charAt(0) == '+' || x._1.charAt(0) == '-') x._1.drop(1) else x._1
      val beg = str.charAt(0)
      val end = str.charAt(str.length - 1)
      // skip the sequences starts with N and ends with N
      if (beg != 'N' || end != 'N') {
        var b = 0
        var e = str.length
        if (beg == 'N') {
          b = 1
          while (str.charAt(b) == 'N') b += 1
        } else if (end == 'N') {
          e -= 1
          while (str.charAt(e - 1) == 'N') e -= 1
        }
        _split(str, b, e, x._2)
      }
    }

    buf.iterator
  }

  private def combine(x: String, y: String): String = {
    if (x > y)
      y.drop(1) + x.drop(1)
    else
      x.drop(1) + y.drop(1)
  }
}
