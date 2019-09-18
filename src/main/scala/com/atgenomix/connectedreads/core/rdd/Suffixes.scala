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

import com.atgenomix.connectedreads.core.model.TwoBitIndexedSeq

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Suffixes {
  implicit def charToByte(c: Char): Byte = c match {
    case 'A' => 0x01
    case 'C' => 0x02
    case 'G' => 0x04
    case 'N' => 0x08
    case 'T' => 0x10
    case '$' => 0x20
    case _ => throw new IllegalArgumentException(c.toString)
  }

  def index(iter: Iterator[TwoBitIndexedSeq], pl: Short): Iterator[TwoBitIndexedSeq] = {
    val buf = ArrayBuffer.empty[TwoBitIndexedSeq]
    val ststack = new mutable.Stack[Short]
    ststack.push(0)

    // Records in the same subtree will be collected into a list
    // as an item in stack, i.e. under the same parent node.
    val llstack = new mutable.Stack[ArrayBuffer[TwoBitIndexedSeq]] // leaf list stack
    llstack.push(new ArrayBuffer[TwoBitIndexedSeq]())


    def calc(prev: TwoBitIndexedSeq, curr: TwoBitIndexedSeq): TwoBitIndexedSeq = {
      def updatelp(curr: TwoBitIndexedSeq, next: Byte, c: Byte): Unit = {
        if ((next & c) == 0) {
          val i = llstack.top.iterator
          while (i.hasNext) {
            val gs = i.next
            if (gs.p1 == curr.p1) gs.next = (next | c).toByte
          }
        }
        if (llstack.size > 1) {
          val preceding = llstack.drop(1).head
          preceding.foreach(elem => {
            if (curr.pa == elem.p1 && curr.codePointAt(curr.pa) == elem.codePointAt(elem.p1)) {
              elem.lp = false
            }
          })
        }
      }
      
      if (curr.p1 == 0) {
        curr.p1 = pl
        curr.pa = 0
      }
      else if (curr.pa == prev.pa) {
        if (llstack.top.isEmpty) {
          prev.next = prev.charAt(prev.p1)
          llstack.top += prev
        }
        
        var next = llstack.top.head.next
        
        if (curr.codePointAt(curr.pa) != llstack.top.last.codePointAt(llstack.top.last.pa)) {
          flush(llstack.top)
          llstack.top.clear()
          next = 0.toByte
        }

        llstack.top += curr
        updatelp(curr, next, curr.charAt(curr.p1))
      }
      else if (curr.pa > prev.pa) {
        if (curr.pa == curr.p1) {
          curr.pa = prev.pa
          
          if (llstack.top.isEmpty) {
            prev.next = prev.charAt(prev.p1)
            llstack.top += prev
          }
          
          if (curr.codePointAt(curr.p1) != llstack.top.last.codePointAt(llstack.top.last.p1)) {
            llstack.top += curr
            updatelp(curr, llstack.top.head.next, curr.charAt(curr.p1))
          }
        }
        else if (curr.pa < curr.p1) {
          val lb = new ArrayBuffer[TwoBitIndexedSeq]()
          llstack.push(lb += curr)
          ststack.push(curr.pa)
          updatelp(curr, 0, curr.charAt(curr.p1))
        }
        else throw new RuntimeException("parent pos > child pos")
      }
      else if (curr.pa < prev.pa) {
        while (curr.pa < ststack.top) {
          ststack.pop()
          flush(llstack.top)
          llstack.pop()
        }

        assert(curr.pa == ststack.top)

        curr.pa = ststack.top

        flush(llstack.top)
        llstack.pop()

        val lb = new ArrayBuffer[TwoBitIndexedSeq]()
        llstack.push(lb += curr)
        updatelp(curr, 0, curr.charAt(curr.p1))
      }

      curr
    }

    def flush(lb: ArrayBuffer[TwoBitIndexedSeq]): Unit = {
      var gs: TwoBitIndexedSeq = lb.find(_.lp == -1).orNull // get the first internal gs if it exists
      for (i <- lb if i.lp) {
        buf += i
        if (gs != null && gs.p1 == i.p1) gs = null
      }
      if (gs != null) buf += gs // add to buf if gs passed all checking
    }

    var prev: TwoBitIndexedSeq = if (iter.hasNext) iter.next else null
    if (prev != null) {
      prev.pa = ststack.top
      // If iter has only one element, then it should also be outputted to output buffer
      if (!iter.hasNext) {
        prev.p1 = pl.toShort
        buf += prev
      }
    }

    while (iter.hasNext) {
      val curr = iter.next
      val cpl = prev.commonPrefix(curr, pl)
      if (cpl > prev.pa) prev = calc(prev, prev.copy(pa = cpl, lp = true))

      curr.pa = cpl
      curr.lp = true // once a leaf, always a leaf
      
      prev = calc(prev.copy(lp = true), curr)
    } // EOW

    llstack.foreach(flush)
    buf.iterator
  }

  def edges(iter: Iterator[TwoBitIndexedSeq], pl: Short): Iterator[(Array[Byte], Array[Byte], Short, Short, Byte, ArrayBuffer[Long])] = {
    val emptyArrByte = Array[Byte]()
    val buf = new ArrayBuffer[(Array[Byte], Array[Byte], Short, Short, Byte, ArrayBuffer[Long])]()
    // initialize prev's values for saving the null check in the following while loop
    var p: TwoBitIndexedSeq = null
    while (iter.hasNext) {
      val r = iter.next

      val vpa = if (r.pa == 0) emptyArrByte else r.subsequence(0, r.pa)
      val vp1 = r.subsequence(0, r.p1)

      if (!r.lp) {
        buf += ((vpa, vp1, r.pa, r.p1, r.next, null)) // pa => p1

        if (r.p1 > pl) {
          val vsl = r.subsequence(1, r.p1)
          buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl
        }
      } else {
        val el = r.length - r.p1 // length of leaf edge

        if (p == null || r.p1 != p.p1 || r.pa != p.pa) {
          buf += ((vpa, vp1, r.pa, r.p1, r.next, if (el == 1) r.pos else null)) // pa => p1
          if (r.p1 > pl) {
            val vsl = r.subsequence(1, r.p1)
            buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl (next = 0)
          }
        }

        if (el > 1) {
          val vlp = r.subsequence(0, r.length - 1) // until $
          buf += ((vp1, vlp, r.p1, (r.length - 1).toShort, 0xFF.toByte, r.pos)) // p1 => lp
        }

        p = r
      }
    } // EOW
    buf.iterator
  }
  
  def edges2(iter: Iterator[TwoBitIndexedSeq], pl: Short): Iterator[(Array[Byte], Array[Byte], Short, Short, Byte, ArrayBuffer[Long])] = {
    val buf = new ArrayBuffer[(Array[Byte], Array[Byte], Short, Short, Byte, ArrayBuffer[Long])]()
  
    val pastack = new mutable.Stack[(Short, Array[Byte])]
    pastack.push((0, Array[Byte]()))
    
    var p: TwoBitIndexedSeq = null
    while (iter.hasNext) {
      val r = iter.next
      
      if (r.pa > pastack.top._1) {
        pastack.push((r.pa, r.subsequence(0, r.pa)))
      } else {
        while (r.pa < pastack.top._1) pastack.pop()
      }
      
      assert(r.subsequence(0, r.pa).mkString == pastack.top._2.mkString)
      
      val vpa = pastack.top._2
      val vp1 = r.subsequence(0, r.p1)
      
      if (!r.lp) {
        buf += ((vpa, vp1, r.pa, r.p1, r.next, null)) // pa => p1
        
        if (r.p1 > pl) {
          val vsl = r.subsequence(1, r.p1)
          buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl
        }
      } else {
        val el = r.length - r.p1 // length of leaf edge
        
        if (p == null || r.p1 != p.p1 || r.pa != p.pa) {
          buf += ((vpa, vp1, r.pa, r.p1, r.next, if (el == 1) r.pos else null)) // pa => p1
          if (r.p1 > pl) {
            val vsl = r.subsequence(1, r.p1)
            buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl (next = 0)
          }
        }
        
        if (el > 1) {
          val vlp = r.subsequence(0, r.length - 1) // until $
          buf += ((vp1, vlp, r.p1, (r.length - 1).toShort, 0xFF.toByte, r.pos)) // p1 => lp
        }
        
        p = r
      }
    } // EOW
    buf.iterator
  }
}
