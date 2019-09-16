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

import com.atgenomix.connectedreads.core.model.{TwoBitIndexedSeq, TwoBitSufSeq}

import scala.collection.mutable.ArrayBuffer

/**
  * Partition prefix and Identify the longest common prefix in successively pair-wise order.
  */
object CommonPrefix {
  def run(sorted: Iterator[TwoBitSufSeq], pl: Short): Iterator[TwoBitIndexedSeq] = {
    val buf = ArrayBuffer.empty[TwoBitIndexedSeq]
    var prev = if (sorted.hasNext) sorted.next else null
    if (prev != null && !sorted.hasNext) // prev._2: offset has been added to position in expansion stage
      buf += TwoBitIndexedSeq(seq = prev.seq, offset = prev.offset, length = prev.length,
        pos = ArrayBuffer[Long](prev.pos))

    while (sorted.hasNext) {
      val curr = sorted.next()
      
      val p1 = prev.commonPrefix(curr, pl)

      if (p1 == prev.length && prev.length == curr.length) {
        // If buf.isEmpty and p1 = len1 = len2, skip this one (prev = curr) and do nothing
        // e.g., p1 = len1 = len2 = 13,  (TCGAAAAAAAAA$, TCGAAAAAAAAA$)
        // curr._2: offset has been added to position in expansion stage
        if (buf.nonEmpty) buf.last.pos += curr.pos
        else {
          buf += TwoBitIndexedSeq(seq = prev.seq, offset = prev.offset, length = prev.length, p1 = pl,
            pos = ArrayBuffer[Long](prev.pos, curr.pos))
        }
      }
      else {
        if (buf.isEmpty) {
          buf += TwoBitIndexedSeq(seq = prev.seq, offset = prev.offset, p1 = p1, length = prev.length,
            pos = ArrayBuffer[Long](prev.pos))
        } else if (p1 != buf.last.p1) {
          buf += buf.last.copy(p1 = p1)
        }
        buf += TwoBitIndexedSeq(seq = curr.seq, offset = curr.offset, length = curr.length, p1 = p1,
          pos = ArrayBuffer[Long](curr.pos))
      }

      prev = curr
    } // EOW

    buf.iterator
  }
}