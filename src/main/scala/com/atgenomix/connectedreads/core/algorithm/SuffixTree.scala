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

import com.atgenomix.connectedreads.core.model._

import scala.annotation.switch
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SuffixTree extends Serializable {
  
  implicit def charToByte(c: Char): Byte = c match {
    case 'A' => 0x01
    case 'C' => 0x02
    case 'G' => 0x04
    case 'N' => 0x08
    case 'T' => 0x10
    case '$' => 0x20
    case _ => throw new IllegalArgumentException(c.toString)
  }
  
  object TraverseDirection extends Enumeration {
    type TraverseDirection = Value
    val ST_UPWARD, ST_DOWNWARD, ST_BRANCH, ST_ROOT, ST_DOWNWARD_LEAF, ST_UPWARD_LEAF, ST_BRANCH_LEAF = Value
  }
  
  /**
    * Identity callback function for indexSS algorithm
    *
    * @param a Collection of TwoBitIndexedSeq objects done first iteration of indexing
    * @return TwoBitIndexedSeq collection for output
    */
  private def identitySS(a: TwoBitIndexedSeq):
  Option[TwoBitIndexedSeq] = Some(a)
  
  /**
    * Identity callback function for indexIS algorithm
    *
    * @param a Collection of TwoBitIndexedSeq objects at the same branch level (p1), done indexing
    * @param b 2-dimension array containing the unique sequence counts
    *          for each A/T/C/G/$ branch at the branch point (p1)
    * @param c Tree traversal direction
    * @return TwoBitIndexedSeq collection for output
    */
  def identityISBuf(a: ArrayBuffer[TwoBitIndexedSeq], b: Array[Array[Int]], c: TraverseDirection.Value):
  Option[ArrayBuffer[TwoBitIndexedSeq]] = Some(a)
  
  def identityISIter(a: Iterator[TwoBitIndexedSeq], b: Array[Array[Int]], c: TraverseDirection.Value):
  Option[Iterator[TwoBitIndexedSeq]] = Some(a)
  
  private def commonPrefix(a: TwoBitCompactSeq, b: TwoBitCompactSeq, min: Int, skip: Short):
  Short = {
    var l = min
    var i = skip / 4
    val e = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < e && (a.seq(i + a.offset) == b.seq(i + b.offset))) i += 1
    if (i < e) {
      l = i * 4
      while (l < min && a.codePointAt(l) == b.codePointAt(l)) l += 1
    }
    l.toShort
  }
  
  /**
    * Compute the longest common prefix of two successive TwoBitSufSeq objects
    *
    * @param a
    * @param b
    * @param skip the length of prefix to skip comparison
    * @return the longest common prefix between a and b
    */
  private def commonPrefixSS(a: TwoBitCompactSeq, b: TwoBitCompactSeq, skip: Short = 0):
  Short = {
    val m = scala.math.min(a.length, b.length)
    commonPrefix(a, b, m, skip)
  }
  
  /**
    * Compute the second longest common prefix of two TwoBitIndexedSeq objects
    *
    * @param a
    * @param b
    * @param skip the length of prefix to skip comparison
    * @return the longest common prefix between a and b until p1
    */
  private def commonPrefixIS(a: TwoBitIndexedSeq, b: TwoBitIndexedSeq, skip: Short = 0):
  Short = {
    val m = scala.math.min(a.p1, b.p1)
    commonPrefix(a, b, m, skip)
  }
  
  /**
    * Comparison function for sorting TwoBitSufSeq objects based on natural sequence order.
    * This function is usually used in Array.sortWith((left, right) => SuffixTree.compareSS(left, right)).
    *
    * @param l the left operand of the comparison
    * @param r the right operand of the comparison
    * @return true if left is less than right; false, otherwise
    */
  def compareSS(l: TwoBitCompactSeq, r: TwoBitCompactSeq):
  Boolean = {
    val min: Int = if (l.length < r.length) l.length else r.length
    var cmp: Int = 0
    var i = 0
    val end = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < end && cmp == 0) {
      if (l.seq(i + l.offset) < r.seq(i + r.offset)) cmp = -1
      else if (l.seq(i + l.offset) > r.seq(i + r.offset)) cmp = 1
      else i += 1
    }
    
    if (cmp == 0) {
      if (l.length < r.length) true
      else if (l.length > r.length) false
      else false
    }
    else if (cmp < 0) {
      if (((l.seq(i + l.offset) & 0x80) ^ (r.seq(i + r.offset) & 0x80)) != 0) false
      else true
    }
    else {
      if (((l.seq(i + l.offset) & 0x80) ^ (r.seq(i + r.offset) & 0x80)) != 0) true
      else false
    }
  }
  
  /**
    * Comparison function for sorting TwoBitIndexedSeq objects based on char > lcp > $ order.
    * This function is usually used in Array.sortWith((left, right) => SuffixTree.compareIS(left, right)).
    *
    * @param l the left operand of the comparison
    * @param r the right operand of the comparison
    * @return true if left is less than right; false, otherwise
    */
  def compareIS(l: TwoBitIndexedSeq, r: TwoBitIndexedSeq):
  Boolean = {
    val min: Int = if (l.p1 < r.p1) l.p1 else r.p1
    var cmp: Int = 0
    var i = 0
    val end: Int = min / 4
    while (i < end && cmp == 0) {
      if (l.seq(i + l.offset) < r.seq(i + r.offset)) cmp = -1
      else if (l.seq(i + l.offset) > r.seq(i + r.offset)) cmp = 1
      i += 1
    }
    if (cmp != 0) {
      if (((l.seq(i + l.offset - 1) & 0x80) ^ (r.seq(i + r.offset - 1) & 0x80)) != 0) cmp = -cmp
    }
    else if (min % 4 != 0) {
      i = i * 4
      while (i < min && cmp == 0) {
        val cl = l.codePointAt(i)
        val cr = r.codePointAt(i)
        if (cl < cr) cmp = -1
        else if (cl > cr) cmp = 1
        i += 1
      }
    }

    if (cmp == 0) {
      if (l.p1 < r.p1) true
      else if (l.p1 > r.p1) false
      else false
    }
    else if (cmp < 0) true
    else false
  }

  /**
    * Indexing function for identifying the longest common prefix of TwoBitSufSeq objects.
    *
    * @param ss Iterator of sorted TwoBitSufSeq collection
    * @param pl length of known common prefix in ss sequences
    * @param f Callback function when new TwoBitIndexedSeq is created, ex: setting idx value
    * @return TwoBitIndexedSeq collection indexed by their longest common prefix
    */
  @deprecated("This will be removed and use indexSufSeq instead", "GraphSeq 0.9")
  def indexSS(ss: Iterator[TwoBitSufSeq], pl: Short = 0,
              f: TwoBitIndexedSeq => Option[TwoBitIndexedSeq] = identitySS):
  Iterator[TwoBitIndexedSeq] = {
    val out = ArrayBuffer.empty[TwoBitIndexedSeq]
    
    var pre = if (ss.hasNext) ss.next else null
    if (pre != null) {
      f(TwoBitIndexedSeq(seq = pre.seq, offset = pre.offset, length = pre.length,
        p1 = pl, pos = ArrayBuffer[Long](pre.pos))) match {
        case Some(o) => out += o
        case None =>
      }
    }
    
    while (ss.hasNext) {
      val cur = ss.next()
      val p1 = commonPrefixSS(pre, cur, pl)

      // identical sequence handling
      if (p1 == pre.length && pre.length == cur.length) {
        out.last.pos.append(cur.pos)
      }
      else {
        if (p1 != out.last.p1) {
          // new lcp identified, duplicate pre, then update lcp of the new pre
          f(out.last.copy(p1 = p1)) match {
            case Some(o) => out += o
            case None =>
          }
        }
        
        f(TwoBitIndexedSeq(seq = cur.seq, offset = cur.offset, length = cur.length, p1 = p1,
          pos = ArrayBuffer[Long](cur.pos))) match  {
          case Some(o) => out += o
          case None =>
        }
      }
      
      pre = cur
    }
    out.iterator
  }
  
  /**
    * Indexing function for identifying the longest common prefix of successive
    * TwoBitSufSeq objects by using iterator-to-iterator transformation.
    *
    * @param iter Iterator of sorted TwoBitSufSeq collection
    * @param pl length of known common prefix in ss sequences
    * @param f Callback function when new TwoBitIndexedSeq is created, ex: setting idx value
    * @return TwoBitIndexedSeq collection indexed by their longest common prefix
    */
  def indexSufSeq(iter: Iterator[TwoBitSufSeq], pl: Short = 0,
                  f: TwoBitIndexedSeq => Option[TwoBitIndexedSeq] = identitySS):
  Iterator[TwoBitIndexedSeq] = {
    
    var pre: Option[TwoBitIndexedSeq] = None
    iter
      .flatMap(c => {
        (pre: @switch) match {
          case Some(p) =>
            val p1 = commonPrefixSS(p, c, pl)
            if (p1 == p.length && p.length == c.length) {
              // identical suffix handling
              p.pos.append(c.pos)
              None
            }
            else {
              pre = Some(TwoBitIndexedSeq(
                seq = c.seq, offset = c.offset, length = c.length,
                p1 = p1, pos = ArrayBuffer[Long](c.pos)))
  
              if (p1 != p.p1)
                // new lcp identified, duplicate pre, then update lcp of the new pre
                Array[TwoBitIndexedSeq](p.copy(p1 = p1), pre.get)
              else
                pre
            }
          case None =>
            pre = Some(TwoBitIndexedSeq(
              seq = c.seq, offset = c.offset, length = c.length,
              p1 = pl, pos = ArrayBuffer[Long](c.pos)))
            pre
        }
      })
      .filter(x => {
        f(x) match {
          case Some(_) => true
          case None => false
        }
      })
  }
  
  /**
    * Indexing function for identifying the longest common prefix of TwoBitIndexedSeq
    * suffix array by using iterator-to-iterator transformation.
    *
    * @param iter Iterator of sorted TwoBitIndexedSeq suffix array
    * @param pl length of known common prefix in ss sequences
    * @param f Callback function when new TwoBitIndexedSeq is created, ex: setting idx value
    * @return TwoBitIndexedSeq longest common prefix array
    */
  def indexSuffixArray(iter: Iterator[TwoBitIndexedSeq], pl: Short = 0,
                       f: TwoBitIndexedSeq => Option[TwoBitIndexedSeq] = identitySS):
  Iterator[TwoBitIndexedSeq] = {
  
    var pre: Option[TwoBitIndexedSeq] = None
    iter
      .flatMap(c => {
        (pre: @switch) match {
          case Some(p) =>
            c.p1 = commonPrefixSS(p, c, pl)
            if (c.p1 == p.length && p.length == c.length) {
              // identical suffix handling
              p.pos ++= c.pos
              None
            }
            else {
              pre = Some(c)
              if (c.p1 != p.p1)
              // new lcp identified, duplicate pre, then update lcp of the new pre
                Array(p.copy(p1 = c.p1), c)
              else
                pre
            }
          case None =>
            c.p1 = pl
            pre = Some(c)
            pre
        }
      })
      .filter(x => {
        f(x) match {
          case Some(_) => true
          case None => false
        }
      })
  }
  
  def indexSuffixArrayAndSort(iter: Iterator[TwoBitIndexedSeq], pl: Short = 0,
                              f: TwoBitIndexedSeq => Option[TwoBitIndexedSeq] = identitySS):
  Iterator[TwoBitIndexedSeq] = {
    indexSuffixArray(iter, pl, f)
      .toArray
      .sortWith(compareIS)
      .iterator
  }
  
  /**
    * Algorithm to index parent breakpoint in TwoBitIndexedSeq collection and traverse the constructed suffix tree
    * simultaneously in top-down depth-first fashion, where leaf nodes describe the incoming sequences entries.
    * Leaf node counts can be used to describe read counts on each internal nodes.
    *
    * @param iter Iterator of sorted TwoBitIndexedSeq collection
    * @param pl length of known common prefix in is sequences
    * @param ml Maximal input sequence length
    * @param f Callback function when new TwoBitIndexedSeqs at the same branch level are outputed
    * @return TwoBitIndexedSeq collection represents the complete suffix tree
    */
  @deprecated("This will be removed; use indexLCPArray instead", "GraphSeq 0.9")
  def indexIS[A](iter: Iterator[TwoBitIndexedSeq], pl: Short = 0, ml: Short = 151,
                 f: (ArrayBuffer[TwoBitIndexedSeq], Array[Array[Int]], TraverseDirection.Value) =>
                   Option[ArrayBuffer[A]]):
  Iterator[A] = {
    ///////////////////////////////////////////////////////////////////////////
    // Variables initialization
    ///////////////////////////////////////////////////////////////////////////
    val out = ArrayBuffer.empty[A]
    
    // a matrix recording the sequence counts of given (A/T/C/G) branches at each branch position in sequence
    val bcmatrix = Array.ofDim[Int](ml, 5)
    
    // substree stack recording the parent branches not done indexing
    val ststack = new mutable.Stack[Short]
    ststack.push(0)

    // track the records under the same parent nodes as a list in stack item, leaf list stack
    val llstack = new mutable.Stack[ArrayBuffer[TwoBitIndexedSeq]]
    llstack.push(new ArrayBuffer[TwoBitIndexedSeq]())
  
    ///////////////////////////////////////////////////////////////////////////
    // internal indexing and traversal algorithm
    ///////////////////////////////////////////////////////////////////////////
    def index(pre: TwoBitIndexedSeq, cur: TwoBitIndexedSeq): TwoBitIndexedSeq = {
      
      def updatelp(cur: TwoBitIndexedSeq, next: Byte, c: Byte): Unit = {
        /*
        Do we need to for-loop all lists in stack for updating leaf nodes?
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
            val o = i.next
            if (o.p1 == cur.p1) o.next = (next | c).toByte
          }
        }
        // we always check the parent subtree, i.e., stack.size > 1
        if (llstack.size > 1) {
          val preceding = llstack.drop(1).head
          preceding.foreach(o => {
            if (cur.pa == o.p1 && cur.codePointAt(cur.pa) == o.codePointAt(o.p1)) {
              o.lp = false // is internal node and the outgoing edge is not a leaf edge
            }
          })
        }
      }
      
      /////////////////////////////////////////////////////////////////////////
      // indexing implementation starts here
      if (cur.p1 == 0) {
        /*
        new branch from root
        always start with partition prefix length, parent is root
        */
        cur.p1 = pl
        cur.pa = 0
      }
      else if (cur.pa == pre.pa) {
        /*
        Under the `same parent node` at the same-level subtree.
        Given in the figure shown below are examples of two records share the same parent node.

        [pre]  (AATACTACCTT$,9,4,4,3,9,AGT)             [cur]  (AATAGGGACT$,7,4,4,3,7,)
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

        The 2nd common prefix of pre and cur should have computed
        before pushing it into the top of llstack.
        Since we cannot do it when stack initialized, the following check is needed.
        */
        if (llstack.top.isEmpty) {
          pre.next = pre.charAt(pre.p1)
          llstack.top += pre
        }

        var next = llstack.top.head.next
        if (cur.codePointAt(cur.pa) != llstack.top.last.codePointAt(llstack.top.last.pa)) {
          /*===================================================================
           same parent, different branches, the existing branch is done indexing, so output
           ====================================================================*/
          output(TraverseDirection.ST_BRANCH)
          llstack.top.clear()
          next = 0.toByte
        }

        llstack.top += cur
        updatelp(cur, next, cur.charAt(cur.p1))
      }
      else if (cur.pa > pre.pa) {
        /*=====================================================================
          change subtree downward, i.e. new subtree branch is seen.
         ======================================================================*/
        if (cur.pa == cur.p1) {
          // FAKE downward, still in the same parent as prev's, subtree stack is not changed.
          cur.pa = pre.pa

          if (llstack.top.isEmpty) {
            // special case: the 1st record of iteration
            pre.next = pre.charAt(pre.p1)
            llstack.top += pre
          }

          if (cur.codePointAt(cur.p1) != llstack.top.last.codePointAt(llstack.top.last.p1)) {
            llstack.top += cur
            updatelp(cur, llstack.top.head.next, cur.charAt(cur.p1))
          }
        }
        else if (cur.pa < cur.p1) {
          // actual downward, create a new subtree
          val lb = ArrayBuffer.empty[TwoBitIndexedSeq]
          llstack.push(lb += cur)
          ststack.push(cur.pa)
          updatelp(cur, 0, cur.charAt(cur.p1))
        }
        else throw new RuntimeException("Tree parent branch position is lower than current branch position")
      }
      else if (cur.pa < pre.pa) {
        /*=====================================================================
         change subtree UPWARD, the current subtree is done indexing
         ======================================================================*/
        while (cur.pa < ststack.top) {
          output(TraverseDirection.ST_UPWARD)
          ststack.pop()
          llstack.pop()
        }

        assert(cur.pa == ststack.top)
        
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
        output(TraverseDirection.ST_UPWARD)
        llstack.pop()

        val lb = ArrayBuffer.empty[TwoBitIndexedSeq]
        llstack.push(lb += cur)
        updatelp(cur, 0, cur.charAt(cur.p1))
      }

      cur
    }
  
    ///////////////////////////////////////////////////////////////////////////
    // output current tree indexing result and call user callback
    ///////////////////////////////////////////////////////////////////////////
    def output(dir: TraverseDirection.Value): Unit = {
      val bl = llstack.top
      if (bl.nonEmpty) {
        val ol = ArrayBuffer.empty[TwoBitIndexedSeq]
        var gs = bl.find(_.lp == false).orNull // get the first internal gs if it exists
        val p1 = bl.head.p1
        val pa = bl.head.pa
        val cp = bl.head.codePointAt(pa)

        for (o <- bl if o.lp) {
          ol += o
          if (gs != null && gs.p1 == o.p1) gs = null
          
          // update bcmatrix(i.p1) with corresponding branching info
          // only leaf nodes are considered
          bcmatrix(p1)(o.codePointAt(p1)) =
            if (o.pos.nonEmpty) bcmatrix(p1)(o.codePointAt(p1)) + o.pos.length
            else bcmatrix(p1)(o.codePointAt(p1)) + 1
        }
        
        if (gs != null) ol += gs // add to output list if gs passes all checking
        
        // propagate bcmatrix(p1) content to parent bcmatrix(pa)
        bcmatrix(pa)(cp) = bcmatrix(pa)(cp) + bcmatrix(p1).sum

        f(ol, bcmatrix, dir) match {
          case Some(o) => out ++= o
          case None =>
        }
        
        for (i <- bcmatrix(p1).indices)
          bcmatrix(p1)(i) = 0
      }
    }

    ///////////////////////////////////////////////////////////////////////////
    // The main index algorithm starts here. Initialization before iteration
    ///////////////////////////////////////////////////////////////////////////
    var pre: TwoBitIndexedSeq = if (iter.hasNext) iter.next else null
    if (pre != null) {
      pre.pa = ststack.top
      // If is collection has only one element, then it should also be outputted
      if (!iter.hasNext) pre.p1 = pl.toShort
    }

    while (iter.hasNext) {
      val cur = iter.next
      // already identify 1st common prefix and sorted
      // new records derived from prev and curr in STEP-3
      // identify second longest common prefix til p1
      val cpl = commonPrefixIS(pre, cur, pl)
      
      // add an additional new record is needed because successive common prefixes are different,
      // i.e. subtree change DOWNWARD
      // so we need to create a new record of prev and identify its tree parent.
      if (cpl > pre.pa) pre = index(pre, pre.copy(pa = cpl))

      cur.pa = cpl

      pre = index(pre.copy(), cur)
    }
    
    while (llstack.nonEmpty) {
      output(TraverseDirection.ST_BRANCH)
      llstack.pop()
    }
    
    out.iterator
  }
  
  /**
    * Index longest common prefix array returned from @indexSuffixArrayAndSort and traverse the constructed suffix tree
    * simultaneously in top-down depth-first fashion, where leaf nodes describe the incoming sequences entries.
    * Leaf node counts can be used to describe read counts on each internal nodes.
    *
    * @param iter Iterator of sorted TwoBitIndexedSeq LCP array
    * @param pl length of known common prefix in is sequences
    * @param ml Maximal input sequence length
    * @param f Callback function when new TwoBitIndexedSeqs at the same branch level are outputed
    * @return TwoBitIndexedSeq array representing the complete suffix tree
    */
  def indexLCPArray[A](iter: Iterator[TwoBitIndexedSeq], pl: Short = 0, ml: Short = 151,
                       f: (Iterator[TwoBitIndexedSeq], Array[Array[Int]], TraverseDirection.Value) =>
                         Option[Iterator[A]]):
  Iterator[A] = {
    // a matrix recording the sequence counts of given (A/T/C/G/$)
    // branches at each branch position in sequence
    val contab = Array.ofDim[Int](ml, 5)
    
    // subtree stack recording the parent branches not done indexing
    val ststack = new mutable.Stack[Short]
    
    // track the records under the same parent nodes as a list in stack item, leaf list stack
    val llstack = new mutable.Stack[ArrayBuffer[TwoBitIndexedSeq]]
    
    ///////////////////////////////////////////////////////////////////////////
    // internal indexing and traversal algorithm
    ///////////////////////////////////////////////////////////////////////////
    def index(pre: TwoBitIndexedSeq, cur: TwoBitIndexedSeq, out: ArrayBuffer[A]):
    Option[TwoBitIndexedSeq] = {
      
      def updatelp(cur: TwoBitIndexedSeq, next: Byte, c: Byte): Unit = {
        /*
        Do we need to for-loop all lists in stack for updating leaf nodes?
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
          llstack.top.foreach(o => {
            if (o.p1 == cur.p1) o.next = (next | c).toByte
          })
        }
        // we always check the parent subtree, i.e., stack.size > 1
        if (llstack.size > 1) {
          val preceding = llstack.drop(1).head
          preceding.foreach(o => {
            if (cur.pa == o.p1 && cur.codePointAt(cur.pa) == o.codePointAt(o.p1)) {
              o.lp = false // is internal node and the outgoing edge is not a leaf edge
            }
          })
        }
      }
      
      /////////////////////////////////////////////////////////////////////////
      // indexing implementation starts here
      if (cur.p1 == 0) {
        /*
        new branch from root
        always start with partition prefix length, parent is root
        */
        cur.p1 = pl
        cur.pa = 0
      }
      else if (cur.pa == pre.pa) {
        /*
        Under the `same parent node` at the same-level subtree.
        Given in the figure shown below are examples of two records share the same parent node.

        [pre]  (AATACTACCTT$,9,4,4,3,9,AGT)             [cur]  (AATAGGGACT$,7,4,4,3,7,)
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

        The 2nd common prefix of pre and cur should have computed
        before pushing it into the top of llstack.
        Since we cannot do it when stack initialized, the following check is needed.
        */
        if (llstack.top.isEmpty) {
          pre.next = pre.charAt(pre.p1)
          llstack.top += pre
        }
        
        var next = llstack.top.head.next
        if (cur.codePointAt(cur.pa) != llstack.top.last.codePointAt(llstack.top.last.pa)) {
          /*===================================================================
           same parent, different branches, the existing branch is done indexing, so output
           ====================================================================*/
          output(llstack.top, TraverseDirection.ST_BRANCH) match {
            case Some(o) => out ++= o
            case None =>
          }
          
          llstack.top.clear()
          next = 0.toByte
        }
        
        llstack.top += cur
        updatelp(cur, next, cur.charAt(cur.p1))
      }
      else if (cur.pa > pre.pa) {
        /*=====================================================================
          change subtree downward, i.e. new subtree branch is seen.
         ======================================================================*/
        if (cur.pa == cur.p1) {
          // FAKE downward, still in the same parent as prev's, subtree stack is not changed.
          cur.pa = pre.pa
          
          if (llstack.top.isEmpty) {
            // special case: the 1st record of iteration
            pre.next = pre.charAt(pre.p1)
            llstack.top += pre
          }
          
          if (cur.codePointAt(cur.p1) != llstack.top.last.codePointAt(llstack.top.last.p1)) {
            llstack.top += cur
            updatelp(cur, llstack.top.head.next, cur.charAt(cur.p1))
          }
        }
        else if (cur.pa < cur.p1) {
          // actual downward, create a new subtree
          val lb = ArrayBuffer.empty[TwoBitIndexedSeq]
          llstack.push(lb += cur)
          ststack.push(cur.pa)
          updatelp(cur, 0, cur.charAt(cur.p1))
        }
        else throw new RuntimeException("Tree parent branch position is lower than current branch position")
      }
      else if (cur.pa < pre.pa) {
        /*=====================================================================
         change subtree UPWARD, the current subtree is done indexing
         ======================================================================*/
        while (cur.pa < ststack.top) {
          output(llstack.pop, TraverseDirection.ST_UPWARD) match {
            case Some(o) => out ++= o
            case None =>
          }
          ststack.pop()
        }
        
        assert(cur.pa == ststack.top)
        
        /*
        Still the UPWARD case, for updating the next info, we add a new item in the llstack.
        Before the successive process, we need to pop again to reach the correct level.
         */
        output(llstack.pop, TraverseDirection.ST_UPWARD) match {
          case Some(x) => out ++= x
          case None =>
        }
        
        llstack.push(ArrayBuffer[TwoBitIndexedSeq](cur))
        updatelp(cur, 0, cur.charAt(cur.p1))
      }
      
      Some(cur)
    }
    
    ///////////////////////////////////////////////////////////////////////////
    // output current tree indexing result and call user callback
    ///////////////////////////////////////////////////////////////////////////
    def output(buf: ArrayBuffer[TwoBitIndexedSeq], dir: TraverseDirection.Value):
    Option[Iterator[A]] = {
      
      def updatetab(n: TwoBitIndexedSeq, itr: Iterator[TwoBitIndexedSeq]): Option[Iterator[A]] = {
        // propagate contab(p1) content to parent contab(pa)
        contab(n.pa)(n.codePointAt(n.pa)) = contab(n.pa)(n.codePointAt(n.pa)) + contab(n.p1).sum
        val ret = f(itr, contab, dir)
        contab(n.p1).indices.foreach(i => contab(n.p1)(i) = 0)
        ret
      }
      
      val inn = buf.find(_.lp == false) // first internal node
      val lfn = buf.find(_.lp == true)  // first leaf node
      (inn, lfn) match {
        case (None, Some(l)) =>
          // only leaf nodes
          buf.foreach(x => {
            contab(x.p1)(x.codePointAt(x.p1)) =
              if (x.pos.nonEmpty) contab(x.p1)(x.codePointAt(x.p1)) + x.pos.length
              else contab(x.p1)(x.codePointAt(x.p1)) + 1
          })
          updatetab(l, buf.iterator)
        case (Some(n), None) =>
          // only internal nodes
          updatetab(n, buf.iterator)
        case (Some(_), Some(l)) =>
          // both internal and leaf nodes, need to remove redundant internal nodes
          val bl = buf.filter(x => {
            if (x.lp) {
              contab(x.p1)(x.codePointAt(x.p1)) =
                if (x.pos.nonEmpty) contab(x.p1)(x.codePointAt(x.p1)) + x.pos.length
                else contab(x.p1)(x.codePointAt(x.p1)) + 1
      
              x.lp
            }
            else
              x.p1 != l.p1
          })
          updatetab(l, bl.iterator)
        case _ =>
          None
      }
    }
    
    ///////////////////////////////////////////////////////////////////////////
    // The main index algorithm starts here. Initialization before iteration
    ///////////////////////////////////////////////////////////////////////////
    ststack.push(0)
    llstack.push(new ArrayBuffer[TwoBitIndexedSeq]())
    var pre: Option[TwoBitIndexedSeq] = None
    iter
      .flatMap(c => {
        (pre: @switch) match {
          case Some(p) =>
            // already identify 1st common prefix and sorted
            // identify second longest common prefix til p1
            val buf = ArrayBuffer.empty[A]
            val pa = commonPrefixIS(p, c, pl)
            if (pa > p.pa) {
              pre = index(p, p.copy(pa = pa), buf)
            }
            c.pa = pa
            pre = index(pre.get.copy(), c, buf)
            if (buf.isEmpty) None else buf
          case None =>
            c.pa = ststack.top
            pre = Some(c)
            None
        }
      })
      .++(llstack
        .flatMap(ll => {
          output(ll, TraverseDirection.ST_BRANCH_LEAF) match {
            case Some(o) => o
            case None => None
          }
        })
      )
  }
  
  def indexLCPArray2BIS(iter: Iterator[TwoBitIndexedSeq], pl: Short = 0, ml: Short = 151,
                        f: (Iterator[TwoBitIndexedSeq], Array[Array[Int]], TraverseDirection.Value) =>
                          Option[Iterator[TwoBitIndexedSeq]] = identityISIter):
  Iterator[TwoBitIndexedSeq] = {
    indexLCPArray[TwoBitIndexedSeq](iter, pl, ml, f)
  }
  
  /**
    * Traverse the suffix tree in TwoBitIndexedSeq dataset in depth-first fashion.
    * @param iter Iterator of sorted TwoBitIndexedSeq collection
    * @param f Callback function when visiting every node.
    * @return The dataset of custom type A returned from user-defined callback function.
    */
  def traverseDepthFirst[A >: TwoBitIndexedSeq](iter: Iterator[TwoBitIndexedSeq],
                                                f: (TwoBitIndexedSeq, TwoBitIndexedSeq, TraverseDirection.Value) =>
                                                  Option[A])
                                               (implicit C: ClassTag[A]):
  Iterator[A] = {
    var pre: TwoBitIndexedSeq = null
    iter
      .flatMap(cur => {
        val out = (pre, cur) match {
          //=====================================================================
          // branch from the root
          //=====================================================================
          case (p, c) if c.pa == 0 =>
            f(p, c, TraverseDirection.ST_ROOT)
          //=====================================================================
          // same parent node with different child branch
          //=====================================================================
          case (p, c) if c.pa == p.pa =>
            if (c.lp)
              f(p, c, TraverseDirection.ST_BRANCH_LEAF)
            else
              f(p, c, TraverseDirection.ST_BRANCH)
          //=====================================================================
          // change subtree downward, i.e. new subtree branch is seen.
          //=====================================================================
          case (p, c) if c.pa > p.pa =>
            if (c.lp)
              f(p, c, TraverseDirection.ST_DOWNWARD_LEAF)
            else
              f(p, c, TraverseDirection.ST_DOWNWARD)
          //=====================================================================
          // change subtree upward, the current subtree is done traversal
          //=====================================================================
          case (p, c) if c.pa < p.pa =>
            if (c.lp)
              f(p, c, TraverseDirection.ST_UPWARD_LEAF)
            else
              f(p, c, TraverseDirection.ST_UPWARD)
        }
        pre = cur
        out match {
          case Some(o) => Array[A](o)
          case None => None
        }
      })
  }
}
