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

package com.atgenomix.connectedreads.core.util

import ArrayByteUtils._
import scala.collection.mutable.ArrayBuffer

object SuffixUtils {
  private def _prefidx(s: String, i: Int, l: Int): Long = {
    var idx = 0
    var j = i
    while (j < l) {
      idx = 11 * idx + s.codePointAt(j)
      j += 1
    }
    idx
  }

  def suffix(s: String, p: Long, pl: Int, min: Int): Iterator[(Array[Byte], (Long, Array[Short], Short))] = {
    val map = scala.collection.mutable.LongMap.empty[((Array[Byte], Short), Long, Int, ArrayBuffer[Short])]
    var idx = 0
    while (s.length - idx >= min) {
      val prefix = _prefidx(s, idx, idx + pl)
      if (map.contains(prefix)) {
        map(prefix)._4 += (idx - map(prefix)._3).toShort
      } else {
        map += (prefix, (encode(s, idx), p + idx, idx, ArrayBuffer[Short](0)))
      }
      idx += 1
    }
    // (sequence data, (position, offset list, sequence data length))
    map.values.map(v => (v._1._1, (v._2, v._4.toArray, v._1._2))).iterator
  }
}
