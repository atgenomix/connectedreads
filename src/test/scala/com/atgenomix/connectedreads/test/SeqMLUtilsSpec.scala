package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.util.SeqMLUtils._
import com.atgenomix.connectedreads.test.base.UnitSpec

class SeqMLUtilsSpec extends UnitSpec {
  "The commpn prefix length of `AATACTCCAG$` and `AATAGGGACA$`" should "be 4" in {
    val s = "AATACTCCAG$"
    val t = "AATAGGGACA$"
    val cp = pref(s, t, math.min(s.length, t.length))

    cp shouldBe 4
  }

  it must "correctly find the common prefix of two Array[Byte] within bound" in {
    // AA,CC,TG|... vs AA,CC,GG; `|` represents the bound
    pref2(Array[Byte](17, 34, 70), Array[Byte](17, 34, 102), 6) should be(4)

    // AA,CC,T|A... vs AA,CC,G|A...
    pref2(Array[Byte](17, 34, 65), Array[Byte](17, 34, 97), 5) should be(4)

    // AA,CC,TG|... vs AA,CC,TA|...
    pref2(Array[Byte](17, 34, 70), Array[Byte](17, 34, 65), 6) should be(5)

    // AA,CC,T|G... vs AA,CC,T|A...
    pref2(Array[Byte](17, 34, 70), Array[Byte](17, 34, 65), 5) should be(5)

    // AA,CC,TT|... vs AA,CC,TT|...
    pref2(Array[Byte](17, 34, 68), Array[Byte](17, 34, 68), 6) should be(6)
  }

}
