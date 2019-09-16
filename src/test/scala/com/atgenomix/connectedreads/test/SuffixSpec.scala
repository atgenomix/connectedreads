package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.util.SuffixUtils._
import org.scalatest.{FlatSpec, Matchers}

class SuffixSpec extends FlatSpec with Matchers {
  /*
  The traditional way will make lots suffixes of the input string, here we try to
  aggregate the offset of not repeating prefixs to recude the cost of string space.
  let prefix length to 3 and minsfx is 3 ($ is excluded),

      0   1   2   3   4   5   6   7   8   9   10  11  12  13  14
      A   A   T   A   A   T   G   A   A   T   G   A   T   G   $

      AATAATGAATGATG$     =>     AAT, AATAATGAATGATG$ (0,3,7)
       ATAATGAATGATG$            GAA, GAATGATG$       (0)
        TAATGAATGATG$            ATA, ATAATGAATGATG$  (0)
         AATGAATGATG$            ATG, ATGAATGATG$     (0,4,7)
          ATGAATGATG$            TGA, TGAATGATG$      (0,4)
           TGAATGATG$            TAA, TAATGAATGATG$   (0)
            GAATGATG$            GAT, GATG$           (0)
             AATGATG$
              ATGATG$
               TGATG$
                GATG$
                 ATG$
   */
  "AATAATGAATGATG$" should "work correctly" in {
    // (seq, position, prefix_length, min_suffix_length)
    val suffixes = suffix("AATAATGAATGATG", 100L, 3, 3).toList
    suffixes.find(x => x._1 sameElements Array[Byte](12, 56, 56, -32)).orNull
      ._2._2 should be(Array[Byte](0, 3, 7))
  }
}
