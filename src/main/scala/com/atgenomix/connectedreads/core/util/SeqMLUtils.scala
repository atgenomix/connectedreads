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

object SeqMLUtils {
  // Why not use substring in our scenario?
  // for reference, look here: <a href="http://stackoverflow.com/questions/16123446/java-7-string-substring-complexity">Java 7 String - substring complexity</a>
  def pref(s: String, t: String, bound: Int): Int = {
    var i = 0
    while (i < bound && s(i) == t(i)) i += 1
    i
  }

  def pref2(s: Array[Byte], t: Array[Byte], bound: Int): Int = {
    var i = 0
    while (i < bound / 2 && (s(i) ^ t(i)) == 0) i += 1
    if (i < bound / 2 || bound % 2 != 0) {
      // 1. found the difference before bound
      // 2. reached bound / 2 and bound is odd number
      // In both cases we have to check if the ith upper 4-bit is the same or not
      if ((s(i) ^ t(i)) >= 16) {
        // upper 4-bit is different: AA,CC,TG|... vs AA,CC,GG|...
        // upper 4-bit is different: AA,CC,T|A... vs AA,CC,G|A...
        i * 2
      } else {
        // upper 4-bit is the same and lower 4-bit is different: AA,CC,TG|... vs AA,CC,TA|...
        // upper 4-bit is the same: AA,CC,T|G... vs AA,CC,T|A...
        i * 2 + 1
      }
    } else {
      // reached bound and bound is even number, we are done: AA,CC,TT|... vs AA,CC,TT|...
      i * 2
    }
  }

}
