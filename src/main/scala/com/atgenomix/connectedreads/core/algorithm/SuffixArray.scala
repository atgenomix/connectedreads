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

package com.atgenomix.connectedreads.core.algorithm

import com.atgenomix.connectedreads.core.model.{NumberedReads, PartitionedReads}
import com.atgenomix.connectedreads.core.util.ArrayByteUtils._

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer

object SuffixArray {
  val NUM_EXTRA_PARTITIONS = 20 //disable this feature when assigning NUM_EXTRA_PARTITIONS as zero

  def getExtraPartitions(pl: Int, batch: Int): Int = {
    var v = 0
    if (NUM_EXTRA_PARTITIONS > 0) {
      if (pl == 0) {
        v = 2 * NUM_EXTRA_PARTITIONS
      } else if ((batch == 0) || ( batch == scala.math.pow(4, pl).toInt-1 )) {
        v = NUM_EXTRA_PARTITIONS
      }
    }
    v
  }

  def addReads(read: NumberedReads, prefix: Int, packing_size: Int, read_byte_length: Int, offset: Short,
               suffixLength: Short, cnt: Array[Int], buffer: Array[(Array[Long], Array[Byte], Array[Short])],
               res: ArrayBuffer[(Int, PartitionedReads)], namingBitLen: Int): Unit = {
    if (cnt(prefix) == 0) {
      buffer(prefix) = (new Array[Long](packing_size), new Array[Byte](packing_size*read_byte_length), new Array[Short](packing_size))
    }
    buffer(prefix)._1(cnt(prefix)) =  read.sn | (offset.toLong << namingBitLen)  //(rc+readid) + offset
    buffer(prefix)._3(cnt(prefix)) = suffixLength                     //suffix length
    encode5plus(read.sequence, offset, cnt(prefix), read_byte_length, buffer(prefix)._2)     //sequence
    cnt(prefix) = cnt(prefix) + 1
    if (cnt(prefix) == packing_size) {
      res += ((prefix, PartitionedReads(cnt(prefix), buffer(prefix)._1, buffer(prefix)._2, buffer(prefix)._3)))
      cnt(prefix) = 0
    }
  }

  def addRemainingReads(partitions: Int, cnt: Array[Int], buffer: Array[(Array[Long], Array[Byte], Array[Short])],
                        res: ArrayBuffer[(Int, PartitionedReads)]): Unit = {
    for (i <- 0 until partitions) {
      if (cnt(i) > 0) {
        res += ((i, PartitionedReads(cnt(i), buffer(i)._1, buffer(i)._2, buffer(i)._3)))
      }
    }
  }

  def _rePartition(batch: Int, pid: Int, num_batch: Int, num_regular_partition: Int, seq: String, pos: Int): Int = {
    var v = pid
    var i = 0

    if (NUM_EXTRA_PARTITIONS > 0) {
      if (num_batch == 1) {
        //PL=0
        if (pid == 0) {
          i = 0
          while (i < NUM_EXTRA_PARTITIONS) {
            if (seq.charAt(pos + i) == 'A') {
              v += 1
              i = i + 1
            } else {
              i = NUM_EXTRA_PARTITIONS
            }
          }
        } else if (pid == num_regular_partition - 1) {
          v = NUM_EXTRA_PARTITIONS + 1
          i = 0
          while (i < NUM_EXTRA_PARTITIONS) {
            if (seq.charAt(pos + i) == 'T') {
              v += 1
              i = i + 1
            } else {
              i = NUM_EXTRA_PARTITIONS
            }
          }
        } else {
          v = v + 2 * NUM_EXTRA_PARTITIONS + 1
        }
      } else {
        if (batch == 0) {
          if (pid == 0) {
            i = 0
            while (i < NUM_EXTRA_PARTITIONS) {
              if (seq.charAt(pos + i) == 'A') {
                v += 1
                i = i + 1
              } else {
                i = NUM_EXTRA_PARTITIONS
              }
            }
          } else {
            v = v + NUM_EXTRA_PARTITIONS
          }
        } else if (batch == num_batch - 1) {
          if (pid == num_regular_partition - 1) {
            v = 0
            i = 0
            while (i < NUM_EXTRA_PARTITIONS) {
              if (seq.charAt(pos + i) == 'T') {
                v += 1
                i = i + 1
              } else {
                i = NUM_EXTRA_PARTITIONS
              }
            }
          } else {
            v = v + NUM_EXTRA_PARTITIONS + 1
          }
        }
      }
    }
    v
  }

  // iterator: Iterator[(readId, sequence, rc)] => (prefix2, (readId+offset+rc, sequence, length))
  def build(iterator: Iterator[NumberedReads],
            pl: Int, pl2: Int, min: Int, maxrlen: Int,
            batch: Int, packing_size: Int, numExtraPartitions: Int, namingBitLen: Int):
  Iterator[(Int, PartitionedReads)] = {
    val res = new ArrayBuffer[(Int, PartitionedReads)]()
    var pre_value = 0
    var prefix2 = 0
    var prefix = 0
    var mask = 0

    //multiple-read aggregation
    val read_byte_length: Int = (maxrlen / 4) + 2 // reformat to suffix + prefix with byte-alignment
    val num_batch: Int = scala.math.pow(4, pl).toInt
    val num_regular_partition: Int = scala.math.pow(4, pl2).toInt
    val partitions: Int =  num_regular_partition + getExtraPartitions(pl, batch)
    val buffer: Array[(Array[Long], Array[Byte], Array[Short])] = new Array(partitions)
    val cnt: Array[Int] = new Array(partitions)
    
    if (pl == 0) {
      mask = math.pow(2, 2 * (pl2 - 1)).toInt - 1

      while (iterator.hasNext) {
        val n = iterator.next()
        val last = n.sequence.length - min + 1
        //val (seq, len) = encode(n._2)
        prefix2 = _prefidx(n.sequence, 0, pl2).toInt
        pre_value = prefix2
        if (numExtraPartitions != 0) {
          prefix2 = _rePartition(batch, prefix2, num_batch, num_regular_partition, n.sequence, pl2)
        }
        
        //multiple-read aggregation
        addReads(n, prefix2, packing_size, read_byte_length, 0.toShort,
          n.sequence.length.toShort, cnt, buffer, res, namingBitLen)

        for (idx <- 1 until last) {
          prefix2 = ((pre_value & mask) << 2) | charToInt(n.sequence.charAt(idx + pl2 - 1))
          pre_value = prefix2
          if (numExtraPartitions != 0) {
            prefix2 = _rePartition(batch, prefix2, num_batch, num_regular_partition, n.sequence, idx+pl2)
          }
          addReads(n, prefix2, packing_size, read_byte_length, idx.toShort,
            (n.sequence.length - idx).toShort, cnt, buffer, res, namingBitLen)
        }
      }
    }
    else {
      mask = math.pow(2, 2 * (pl - 1)).toInt - 1

      while (iterator.hasNext) {
        val n = iterator.next()
        val last = n.sequence.length - min + 1
        //val (seq, len) = encode(n._2)
        prefix = _prefidx(n.sequence, 0, pl).toInt
        if (prefix == batch) {
          prefix2 = _prefidx(n.sequence, pl, pl + pl2).toInt
          if (numExtraPartitions != 0) {
            prefix2 = _rePartition(batch, prefix2, num_batch, num_regular_partition, n.sequence, pl+pl2)
          }
          //multiple-read aggregation
          addReads(n, prefix2, packing_size, read_byte_length, 0.toShort,
            n.sequence.length.toShort, cnt, buffer, res, namingBitLen)
        }
        for (idx <- 1 until last) {
          prefix = ((prefix & mask) << 2) | charToInt(n.sequence.charAt(idx + pl - 1))
          if (prefix == batch) {
            prefix2 = _prefidx(n.sequence, idx + pl, idx + pl + pl2).toInt
            if (numExtraPartitions != 0) {
              prefix2 = _rePartition(batch, prefix2, num_batch, num_regular_partition, n.sequence, idx+pl+pl2)
            }
            addReads(n, prefix2, packing_size, read_byte_length, idx.toShort,
              (n.sequence.length - idx).toShort, cnt, buffer, res, namingBitLen)
          }
        }
      }
    }
    //multiple-read aggregation
    addRemainingReads(partitions, cnt, buffer, res)
    
    res.iterator
  }


  def charToInt(c: Char@switch): Int = c match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => throw new IllegalArgumentException
  }

  def charToByte(c: Char@switch): Byte = c match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => throw new IllegalArgumentException
  }

  private def _prefidx(s: String, i: Int, l: Int): Long = {
    var idx = 0
    var j = i
    while (j < l) {
      // eg: TTT => (((0*4 + 3)*4 +3)*4)+3
      idx = (idx << 2) | charToByte(s.charAt(j))
      j += 1
    }
    idx
  }
  
}
