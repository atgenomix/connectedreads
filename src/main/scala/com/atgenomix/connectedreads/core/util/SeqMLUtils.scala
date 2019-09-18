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
  def pref(s: String, t: String, bound: Int): Int = {
    var i = 0
    while (i < bound && s(i) == t(i)) i += 1
    i
  }

  def pref2(s: Array[Byte], t: Array[Byte], bound: Int): Int = {
    var i = 0
    while (i < bound / 2 && (s(i) ^ t(i)) == 0) i += 1
    if (i < bound / 2 || bound % 2 != 0) {
      if ((s(i) ^ t(i)) >= 16) {
        i * 2
      } else {
       i * 2 + 1
      }
    } else {
      i * 2
    }
  }

}
