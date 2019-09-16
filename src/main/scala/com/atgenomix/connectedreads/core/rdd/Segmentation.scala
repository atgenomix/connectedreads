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

/*
Segmentation length = 2L and Overlapping length = L, where L : the maximal length of the query sequence.
Let L = 15 as shown in the following,
           145                195       219
            | 5 | 15 | 15 | 15 | 15 | 10 |
            ------------------------------
                |    2L   |
               idx
                      |   2L   |
                           |   2L   |
                                           5
                               |- - - - -|- -|
                              195
            | 5 |              |  L + 10 |
            | 5 + L  |              | 10 |
                                   210
First, we check whether the start (i.e., 145) of this fragement is divisible by L.
If ture, we generate a segmentation with 2L length from the begining of sequence.
If false, make two segmentations with n and n + L respectively.
To get value of n, count the pos by => ((start/l) + 1)*l then subtract start from it.
Next, the index will be shifted right with L that start to harvest the 2L sequences.
Finally, the remaining may be equal to L or less than 2L. If the length of surplus
equals to L, output the length of fragment is L; otherwise, we have to make two
segments to ensure that no one (i.e., 2L) will be missed.
 */
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

        // for Long (64-bit) data structure: count number of reads from WGS ~40x: 719,267,147.
        // So 32 bit is far more enough to encode read ID and read-pair information.
        // | types (2 bits) | sampleids (14 bits) | chromosome (16 bits) | position in reference (32 bits)
        val cm: Long = contigmask(contig)
        var idx = 0

        // Head: test if the start can be evenly divided by L
        if ((start % l) == 0) {
          // If remainder is equals to 0, just output the seq(0, L) which represents the tail of previous 2L.
          // The prefix character `-` represents the tail of prior fragement,
          // it will be aggregated together data with the same key (i.e., the `+` part)
          // then utilize the character to decide the order within items.
          buf += ((cm | (start - l), "-" + seq.substring(0, l)))
        } else {
          // If remainder greater than 0, makes two strings with length of n and n + L
          // and shift the idx to right with n afterwards.
          val pos = ((start / l) + 1) * l
          val n: Int = (pos - start).toInt // the differences
          idx = idx + n
          buf += (
            (cm | (pos - 2 * l), "-" + seq.substring(0, n)),
            (cm | (pos - l), "-" + seq.substring(0, n + l)))
        }

        // The intermediate section which can be divisible by 2L
        while (idx + 2 * l <= seq.length) {
          buf += ((cm | (idx + start), seq.substring(idx, idx + 2 * l)))
          idx = idx + l // shift the idx to right with L
        }

        // The process of dealing with tail is similar to head. Returns one item
        // if the length of remaining is equals to L, else makes two fragements
        // with related positions.
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


//object Segmentation {
//
//  @Deprecated
//  def segmentAAA(contig: String, seq: String, start: Long, l: Int): Iterator[(Long, String)] = {
//    def contigmask(contig: String): Long = {
//      contig.charAt("chr".length) match {
//        case 'X' => 23L << 32
//        case 'Y' => 24L << 32
//        case 'M' => 25L << 32
//        case _ => contig.drop(3).toLong << 32 // i.e., chr1-22
//      }
//    }
//
//    // for Long (64-bit) data structure: count number of reads from WGS ~40x: 719,267,147.
//    // So 32 bit is far more enough to encode read ID and read-pair information.
//    // | types (2 bits) | sampleids (14 bits) | chromosome (16 bits) | position in reference (32 bits)
//    val cm: Long = contigmask(contig)
//    var idx = 0
//    val buf = new ArrayBuffer[(Long, String)]()
//
//    // Head: test if the start can be evenly divided by L
//    if ((start % l) == 0) {
//      // If remainder is equals to 0, just output the seq(0, L) which represents the tail of previous 2L.
//      // The prefix character `-` represents the tail of prior fragement,
//      // it will be aggregated together data with the same key (i.e., the `+` part)
//      // then utilize the character to decide the order within items.
//      buf += ((cm | (start - l), "-" + seq.substring(0, l)))
//    } else {
//      // If remainder greater than 0, makes two strings with length of n and n + L
//      // and shift the idx to right with n afterwards.
//      val pos = ((start / l) + 1) * l
//      val n: Int = (pos - start).toInt // the differences
//      idx = idx + n
//      buf += (
//        (cm | (pos - 2 * l), "-" + seq.substring(0, n)),
//        (cm | (pos - l), "-" + seq.substring(0, n + l)))
//    }
//
//    // The intermediate section which can be divisible by 2L
//    while (idx + 2 * l <= seq.length) {
//      buf += ((cm | (idx + start), seq.substring(idx, idx + 2 * l)))
//      idx = idx + l // shift the idx to right with L
//    }
//
//    // The process of dealing with tail is similar to head. Returns one item
//    // if the length of remaining is equals to L, else makes two fragements
//    // with related positions.
//    if (seq.length - idx == l) {
//      buf += ((cm | (idx + start), "+" + seq.substring(idx)))
//    }
//    else {
//      buf += (
//        (cm | (start + idx), "+" + seq.substring(idx)),
//        (cm | (start + idx + l), "+" + seq.substring(idx + l)))
//    }
//
//    buf.iterator
//  }
//}