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
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

trait Segmenter {
  def segment: Iterator[(Long, String)]
}

object SegmentFactory {

  def apply[T](s: FileFormat.Value)(args: AnyRef*): T = {
    Class.forName(getClass.getName + s)
      .getConstructors()(0)
      .newInstance(args: _*)
      .asInstanceOf[T]
  }

  private class FASTA(df: Iterator[Row], l: Int) extends Segmenter {

    override def segment: Iterator[(Long, String)] = {
      val buf = new ArrayBuffer[(Long, String)]()
      while (df.hasNext) {
        // 0: contig.contigName, 1: fragmentSequence, 2: fragmentStartPosition
        val x = df.next()
        val contig: String = x.getString(0)
        val seq: String = x.getString(1).toUpperCase
        val start: Long = x.getLong(2)

        val cm: Long = contigmask(contig)
        var idx = 0

        if ((start % l) == 0) {
          buf += ((cm | (start - l), "-" + seq.substring(0, l)))
        } else {
          val pos = ((start / l) + 1) * l
          val n: Int = (pos - start).toInt // the differences
          idx = idx + n
          buf += (
            (cm | (pos - 2 * l), "-" + seq.substring(0, n)),
            (cm | (pos - l), "-" + seq.substring(0, n + l)))
        }

        while (idx + 2 * l <= seq.length) {
          buf += ((cm | (idx + start), seq.substring(idx, idx + 2 * l)))
          idx = idx + l // shift the idx to right with L
        }

        if (seq.length - idx == l) {
          buf += ((cm | (idx + start), "+" + seq.substring(idx)))
        }
        else {
          buf += (
            (cm | (start + idx), "+" + seq.substring(idx)),
            (cm | (start + idx + l), "+" + seq.substring(idx + l)))
        }
      } // EOW

      buf.iterator
    }

    def contigmask(contig: String): Long = {
      contig.charAt("chr".length) match {
        case 'X' => 23L << 32
        case 'Y' => 24L << 32
        case 'M' => 25L << 32
        case _ => contig.drop(3).toLong << 32 // i.e., chr1-22
      }
    }
  }
}


