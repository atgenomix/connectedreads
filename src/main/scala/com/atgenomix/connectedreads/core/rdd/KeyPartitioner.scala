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

import org.apache.spark.Partitioner
import com.atgenomix.connectedreads.core.util.ArrayByteUtils._

import scala.annotation.switch

class KeyPartitioner(pl: Int) extends Partitioner {
  implicit def base5Toint(s: String): Int = {
    var n: Double = 0d
    for (i <- s.indices) {
      val b: Short = s(i) match {
        case 'A' => 0
        case 'C' => 1
        case 'G' => 2
        case 'N' => 3
        case 'T' => 4
        case _ => throw new IllegalArgumentException(s(i).toString)
      }
      n += b * scala.math.pow(5, pl - i - 1)
    }
    n.toInt
  }

  override def getPartition(key: Any): Int = key match {
    case key: Array[Byte] => until(key, pl) % numPartitions
  }

  override def numPartitions: Int = scala.math.pow(5, pl).toInt
}

class KeyPartitioner4(pl: Int, skip: Int = 0) extends Partitioner {
  override def getPartition(key: Any): Int = key match {
    case key: Array[Byte] =>
      var n: Double = 0d
      var i = skip
      while (i < pl) {
        val b = (i & 0x03: @switch) match {
          case 0 => (key(i >> 2) & 0xC0) >> 6
          case 1 => (key(i >> 2) & 0x30) >> 4
          case 2 => (key(i >> 2) & 0x0C) >> 2
          case 3 => (key(i >> 2) & 0x03) >> 0
        }
        // e.g., TTT => 3 * 4^2 + 3 * 4^1 + 3 * 4^0 = 63
        n += b * scala.math.pow(4, pl - i - 1)
        i += 1
      }
      n.toInt % numPartitions
  }

  override def numPartitions: Int = scala.math.pow(4, pl - skip).toInt
}

class KeyPartitioner3(pl: Int, numExtraPartition: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key match {
    case key: Int =>
      key % (numPartitions + numExtraPartition)
  }

  override def numPartitions: Int = scala.math.pow(4, pl).toInt + numExtraPartition
}
