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
        /*
        Do we need to for-loop all lists in stack for updating lead nodes?
        In fact, we only need to update the list prior to the top element stack.
        It's assumed that the first list in stack contains four records,
        including AATACTCCAG$, AATAGGGACA$, AATAGGGTTT$ and AATATTTAGA$.
        Then current record AATACTACCTT$ makes the new branch, moreover,
        AATACTCCAG$ and curr share the same following character `C`.
        So that we can remove the elem from list to avoid further comparison.

                      o
                      |aat
                      o
                      |a
                      o . . . elem => AATACTCCAG$,4,4,3,0,-1,CGT (stack.top.preceding)
                    / |ct
                   /  o . . . curr => AATACTACCTT$,6,4,4,3,6,    (stack.top)
                  /    \
             ctccag$    \
             gggaca$     \
             gggttt$    acctt$
             tttaga$
        
        If the curr.next contains the upcoming char do nothing,
        otherwise append tne char to each element's next.
        */
        if ((next & c) == 0) {
          val i = llstack.top.iterator
          while (i.hasNext) {
            val gs = i.next
            if (gs.p1 == curr.p1) gs.next = (next | c).toByte
          }
        }
        // we always check the parent subtree, i.e., stack.size > 1
        if (llstack.size > 1) {
          val preceding = llstack.drop(1).head
          preceding.foreach(elem => {
            if (curr.pa == elem.p1 && curr.codePointAt(curr.pa) == elem.codePointAt(elem.p1)) {
              elem.lp = false // is internal node and the outgoing edge is not a leaf edge
            }
          })
        }
      }
      
      if (curr.p1 == 0) {
        /*
        new branch from root
        always start with partition prefix length, parent is root
        */
        curr.p1 = pl
        curr.pa = 0
      }
      else if (curr.pa == prev.pa) {
        /*
        Under the `same parent node` at the same-level subtree.
        Given in the figure shown below are examples of two records share the same parent node.
        
        [prev] (AATACTACCTT$,9,4,4,3,9,AGT)             [curr] (AATAGGGACT$,7,4,4,3,7,)
                o                                               o
                |aat                                            |aat
                o . . . gpa = 3                                 o . . . gpa = 3
                |a                                              |a
                o . . . p2 = pa = 4                             o . . . p2 = pa = 4
                |ct                                             |ggg
                o                                               o . . . p1 = 7
                |acc                                           /
                o . . . p1 = 9                                /
                 \                                           /
                  \                                         act$
                   \
                   tt$
        
        The 2nd common prefix of prev and curr should have computed
        before pushing it into the top of llstack.
        Since we cannot do it when stack initialized, the following check is needed.
        */
        if (llstack.top.isEmpty) {
          prev.next = prev.charAt(prev.p1)
          llstack.top += prev
        }
        
        var next = llstack.top.head.next
        
        if (curr.codePointAt(curr.pa) != llstack.top.last.codePointAt(llstack.top.last.pa)) {
          // same parent, different branches, the existing branch is done indexing, so output
          flush(llstack.top)
          llstack.top.clear()
          next = 0.toByte
        }

        llstack.top += curr
        updatelp(curr, next, curr.charAt(curr.p1))
      }
      else if (curr.pa > prev.pa) {
        /*
        change subtree downward, i.e. new subtree branch is seen.
        */
        if (curr.pa == curr.p1) {
          // FAKE downward, still in the same parent as prev's, subtree stack is not changed.
          curr.pa = prev.pa
          
          if (llstack.top.isEmpty) {
            // special case: the 1st record of iteration
            prev.next = prev.charAt(prev.p1)
            llstack.top += prev
          }
          
          if (curr.codePointAt(curr.p1) != llstack.top.last.codePointAt(llstack.top.last.p1)) {
            llstack.top += curr
            updatelp(curr, llstack.top.head.next, curr.charAt(curr.p1))
          }
        }
        else if (curr.pa < curr.p1) {
          // actual downward, create a new subtree
          val lb = new ArrayBuffer[TwoBitIndexedSeq]()
          llstack.push(lb += curr)
          ststack.push(curr.pa)
          updatelp(curr, 0, curr.charAt(curr.p1))
        }
        else throw new RuntimeException("parent pos > child pos")
      }
      else if (curr.pa < prev.pa) {
        /*
         change subtree UPWARD => same parent, but different p1
          */
        while (curr.pa < ststack.top) {
          ststack.pop()
          flush(llstack.top) // add the elements to buf before pop
          llstack.pop()
        }

        assert(curr.pa == ststack.top)

        curr.pa = ststack.top

        /*
        Still the UPWARD case, for updating the next info, we add a new item in the llstack.
        Before the successive process, we need to pop again to reach the correct level.
        For example from our unit-test case:

        prev >> (AATACTACCTT$,9,4,6,4,9,AGT)
        curr >> (AATAGGGACT$,7,4,4,3,7,)

        [Before]
        ListBuffer((AATACTCCAG$,6,6,4,3,6,AC))
        ListBuffer((AATAGGGACA$,4,4,3,0,4,CGT), (AATAGGGTTT$,4,4,3,0,4,CGT), (AATATTTAGA$,4,4,3,0,4,CGT))

        [After]
        ListBuffer((AATAGGGACT$,7,4,4,3,7,))
        ListBuffer((AATAGGGACA$,4,4,3,0,4,CGT), (AATAGGGTTT$,4,4,3,0,4,CGT), (AATATTTAGA$,4,4,3,0,4,CGT))
         */
        flush(llstack.top) // add the elements to buf before pop
        llstack.pop()

        val lb = new ArrayBuffer[TwoBitIndexedSeq]()
        llstack.push(lb += curr)
        updatelp(curr, 0, curr.charAt(curr.p1)) // pass the `next` info
      }

      curr
    }

    def flush(lb: ArrayBuffer[TwoBitIndexedSeq]): Unit = {
      var gs: TwoBitIndexedSeq = lb.find(_.lp == -1).orNull // get the first internal gs if it exists
      // output the leaf nodes to buf and check the prior gs, skip the gs
      // if both leaf and gs have the same p1, i.e., gs is unnecessary record
      for (i <- lb if i.lp) {
        buf += i
        if (gs != null && gs.p1 == i.p1) gs = null
      }
      if (gs != null) buf += gs // add to buf if gs passed all checking
    }

    /*
     The main index algorithm starts here.
     initialization before iteration
      */
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
      // already identify 1st common prefix and sorted
      // new records derived from prev and curr in STEP-3
      // identify second longest common prefix til p1
      val cpl = prev.commonPrefix(curr, pl)
      // add an additional new record is needed because successive common prefixes are different, i.e. subtree change DOWNWARD
      // so we need to create a new record of prev and identify its tree parent and grand parent.
      if (cpl > prev.pa) prev = calc(prev, prev.copy(pa = cpl, lp = true))

      curr.pa = cpl
      curr.lp = true // once a leaf, always a leaf
      
      prev = calc(prev.copy(lp = true), curr)
    } // EOW

    llstack.foreach(flush)
    buf.iterator
  }

  /**
    * Construct dataframes representing edges from suffix index.
    *
    * src: Array[Byte], the unique sequence data from root to parent node.
    * dst: Array[Byte], the unique sequence data from root to child node.
    * src_len: Short, the longest common prefix position of parent node for dst
    * for suffix link this is the length of the parent sequence of dst.
    * dst_len: Short, the length of dst sequence
    * next_bases: Byte, all the first outgoing characters following dst.
    * This is a 8-bit map encoding A (0x01), C (0x02), G (0x04), N (0x08), T (0x10), $ (0x20).
    * where $ is the sequence terminating character.
    * for suffix link edge, this field is always 0x00.
    * for leaf edge, this field is always 0xFF.
    * pos_list: Array[Long], the list of chromosome positions the sequence occurs.
    * This is only stored in leaf edges and terminating internal edges.
    **/
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
        // InEg(src: seq[0:pa], dst: seq[0:p1], p1: Int, pa: Int, gpa: Int, attr(...))
        buf += ((vpa, vp1, r.pa, r.p1, r.next, null)) // pa => p1

        // SlEg(src: seq[0:p1], dst: seq[1:p1], attr(...))
        if (r.p1 > pl) {
          val vsl = r.subsequence(1, r.p1)
          buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl
        }
      } else {
        val el = r.length - r.p1 // length of leaf edge

        if (p == null || r.p1 != p.p1 || r.pa != p.pa) {
          // check the first leaf edge with only $
          // if el != 1, we store the positions in p1 => lp
          // else if el == 1 (i.e., $ edge) we store the positions in the internal nodes to reduce the number of edges
          buf += ((vpa, vp1, r.pa, r.p1, r.next, if (el == 1) r.pos else null)) // pa => p1
          if (r.p1 > pl) {
            val vsl = r.subsequence(1, r.p1)
            buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl (next = 0)
          }
        }

        // exclude leaf edge with only $
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
        // InEg(src: seq[0:pa], dst: seq[0:p1], p1: Int, pa: Int, gpa: Int, attr(...))
        buf += ((vpa, vp1, r.pa, r.p1, r.next, null)) // pa => p1
        
        // SlEg(src: seq[0:p1], dst: seq[1:p1], attr(...))
        if (r.p1 > pl) {
          val vsl = r.subsequence(1, r.p1)
          buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl
        }
      } else {
        val el = r.length - r.p1 // length of leaf edge
        
        if (p == null || r.p1 != p.p1 || r.pa != p.pa) {
          // check the first leaf edge with only $
          // if el != 1, we store the positions in p1 => lp
          // else if el == 1 (i.e., $ edge) we store the positions in the internal nodes to reduce the number of edges
          buf += ((vpa, vp1, r.pa, r.p1, r.next, if (el == 1) r.pos else null)) // pa => p1
          if (r.p1 > pl) {
            val vsl = r.subsequence(1, r.p1)
            buf += ((vp1, vsl, (r.pa - 1).toShort, (r.p1 - 1).toShort, 0x00.toByte, null)) // p1 => sl (next = 0)
          }
        }
        
        // exclude leaf edge with only $
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
