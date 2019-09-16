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

package com.atgenomix.connectedreads.pipeline

import com.atgenomix.connectedreads.cli.SuffixIndexArgs
import com.atgenomix.connectedreads.core.algorithm.{SuffixArray, SuffixTree}
import com.atgenomix.connectedreads.core.model._
import com.atgenomix.connectedreads.core.rdd.KeyPartitioner3
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer


class SuffixIndexPipeline(@transient val spark: SparkSession, args: SuffixIndexArgs,
                          nameBitLen: Int = 36,
                          nameMask:   Long = 0x0000000FFFFFFFFFL,
                          offsetMask: Long = 0x000FFFF000000000L,
                          rcMask:     Long = 0x4000000000000000L) extends Serializable {

  import spark.implicits._
  
  type SA = (Int, Array[Long], Array[Byte], Array[Short])
  
  /**
    * Build a sequence-packed suffix array
    * @param numberedReads
    * @param batch
    * @return dataset of type SA
    */
  def buildSA(numberedReads: Dataset[NumberedReads], batch: Int):
  Dataset[SA] = {
    val num_extra_partitions = SuffixArray.getExtraPartitions(args.pl, batch)
    numberedReads
      .mapPartitions(NumberedReads.makeReverseComplement(_, rcMask))
      .mapPartitions(i =>
        SuffixArray
          .build(i, args.pl, args.pl2, args.minlcp, args.maxrlen, batch,
                 args.packing_size, num_extra_partitions, nameBitLen)
      )
      .rdd
      .partitionBy(new KeyPartitioner3(args.pl2, num_extra_partitions))
      .map(v => (v._2.cnt, v._2.readId, v._2.seq, v._2.suffixLength))
      .toDS()
  }
  
  /**
    * Generate a sorted TwoBitSufSeq dataset
    * @param sa
    * @return
    */
  def generateSA(sa: Iterator[SA]):
  Iterator[TwoBitIndexedSeq] = {
    val rbl = (args.maxrlen / 4) + 2 //reformat bytearray by (suffix + prefix) with byte-alignment
    
    (sa flatMap (x => {
      val ssa = ArrayBuffer.empty[TwoBitIndexedSeq]
      var k = 0
      while (k < x._1) {
        ssa += TwoBitIndexedSeq(
          seq = x._3,
          offset = (k * rbl).toShort, //NOTE: the position of the complete read
          length = x._4(k),
          idx = ((x._2(k) & offsetMask) >> nameBitLen).toShort,
          pos = ArrayBuffer[Long](x._2(k))
        )
        k += 1
      }
      ssa
    }))
      .toArray
      .sortWith(SuffixTree.compareSS)
      .iterator
  }
  
  /**
    * Generate a suffix tree index represented by a sorted TwoBitIndexedSeq dataset
    * @param sss
    * @return
    */
  def generateST(sss: Iterator[TwoBitIndexedSeq]):
  Iterator[TwoBitIndexedSeq] = {
    SuffixTree
      .indexLCPArray2BIS(
        SuffixTree
          .indexSuffixArrayAndSort(sss, (args.pl + args.pl2).toShort),
        (args.pl + args.pl2).toShort,
        args.maxrlen.toShort
      )
  }
}
