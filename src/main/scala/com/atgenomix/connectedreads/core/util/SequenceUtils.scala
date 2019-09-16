
package com.atgenomix.connectedreads.core.util

import java.io.Serializable

import com.atgenomix.connectedreads.cli.GraphSeqArgsBase
import com.atgenomix.connectedreads.core.algorithm.{SuffixArray, SuffixTree}
import com.atgenomix.connectedreads.core.model.{NumberedReads, TwoBitSufSeq}
import com.atgenomix.connectedreads.core.rdd.KeyPartitioner3
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.switch
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object SequenceUtils extends Serializable {

  type SuffixArray = (Int, Array[Long], Array[Byte], Array[Short])

  /**
    * conduct reads reverse-complement expansion, suffix expansion, and repartition
    * @param spark SparkSession object
    * @param batch the length of sequence prefix for data partition; for example, batch==1 => data is partitioned into
    *              4 partitions with prefixes of A, C, G, and T.
    * @param numberedReads input sequence data in the format of (serial_number, Sequence)
    * @param args argument packs
    * @param rcMask bit mask for reverse-complement flag encoding, 0x4000000000000000L by default
    * @param offsetMask bit mask for suffix offset in the original reads encoding, 0x000FFFF000000000L by default
    * @param nameBitLen bit length used for read name encoding, 36 by default
    * @return
    */
  def preprocessing(spark: SparkSession, batch: Int, numberedReads: Dataset[NumberedReads],
                    args: GraphSeqArgsBase, rcMask: Long, offsetMask: Long, nameBitLen: Int): Dataset[TwoBitSufSeq] = {
    import spark.implicits._

    val num_extra_partitions = SuffixArray.getExtraPartitions(args.pl, batch)

    if (args.assignN)
      numberedReads
        .mapPartitions(assignNBase(_, args.maxNCount))
        .mapPartitions(NumberedReads.makeReverseComplement(_, rcMask))
        .mapPartitions(iter =>
          SuffixArray.build(iter, args.pl, args.pl2, args.minlcp, args.maxrlen,
            batch, args.packing_size, num_extra_partitions, nameBitLen))
        .rdd
        .partitionBy(new KeyPartitioner3(args.pl2, num_extra_partitions))
        .map(v => (v._2.cnt, v._2.readId, v._2.seq, v._2.suffixLength))
        .mapPartitions(expandSortSeq(_, args, offsetMask, nameBitLen))
        .toDS()
    else
      numberedReads
        .mapPartitions(NumberedReads.makeReverseComplement(_, rcMask))
        .mapPartitions(iter =>
          SuffixArray.build(iter, args.pl, args.pl2, args.minlcp, args.maxrlen,
            batch, args.packing_size, num_extra_partitions, nameBitLen))
        .rdd
        .partitionBy(new KeyPartitioner3(args.pl2, num_extra_partitions))
        .map(v => (v._2.cnt, v._2.readId, v._2.seq, v._2.suffixLength))
        .mapPartitions(expandSortSeq(_, args, offsetMask, nameBitLen))
        .toDS()
  }

  def assignNBase(iter: Iterator[NumberedReads], maxN: Int): Iterator[NumberedReads] = {
    val alphabet = Array[Char]('A', 'C', 'G', 'T')
    val randGen = scala.util.Random
    
    iter
      .flatMap(x => {
        var countN = 0
        val strBuf = new mutable.StringBuilder()
        for (c <- x.sequence) {
          (c: @switch) match {
            case 'N' =>
              countN += 1
              strBuf += alphabet(randGen.nextInt(4))
            case _ =>
              strBuf += c
          }
        }
        if (countN <= maxN)
          Array(NumberedReads(x.sn, strBuf.toString(), x.qual))
        else
          None
      })
  }

  /**
    * Algorithm that returns the reversed complementary of a DNA-sequence.
    *
    * @param s DNA-sequences
    * @return reversed-complementary DNA-sequence
    */
  def reverseComplementary(s: String): String = {
    val complementary = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C')
    var i = 0
    var j = s.length - 1
    val c = s.toCharArray

    while (i < j) {
      val temp = c(i)
      c(i) = complementary(c(j))
      c(j) = complementary(temp)
      i = i + 1
      j = j - 1
    }

    if (s.length % 2 != 0) c(i) = complementary(c(i))

    String.valueOf(c)
  }

  def expandSortSeq(iter: Iterator[SuffixArray], args: GraphSeqArgsBase, offsetMask: Long, nameBitLen: Int):
  Iterator[TwoBitSufSeq] ={
    val rbl = (args.maxrlen / 4) + 2
    iter
      .flatMap(x => {
        val ssa = ArrayBuffer.empty[TwoBitSufSeq]
        for (k <- 0 until x._1) {
          ssa += TwoBitSufSeq(
            seq = x._3,
            offset = (k * rbl).toShort, //NOTE: the position of the complete read
            length = x._4(k),
            idx = ((x._2(k) & offsetMask) >> nameBitLen).toShort,
            pos = x._2(k)
          )
        }
        ssa
      })
      .toArray
      .sortWith(SuffixTree.compareSS)
      .iterator
  }
}
