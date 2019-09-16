package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.util.ArrayByteUtils._
import org.scalatest.{FlatSpec, Matchers}

class ArrayByteUtilsSpec extends FlatSpec with Matchers {
//  "Both odd and even sequences" should "be encoded properly" in {
//    encode("ACAG") should be(Array[Byte](18, 0), 5)
//    encode("ACGNT") should be(Array[Byte](18, 52, 80), 6)
//  }

  "An Array[Byte](18, 0)" should "be decoded to `AC$`" in {
    decode(Array[Byte](18, 0)) should be("AC$")
    decode(Array[Byte](82, 48)) should be("TCG$")
  }

  "The chars in positions 4 and 5 of Array[Byte](18, 18, 16)" should "be 'A' and '$' respectively" in {
    val b = Array[Byte](18, 18, 16) // AC AC A$
    charAt(b, 4) should be('A')
    charAt(b, 5) should be('$')
  }

  "The prefixes in p1 4 and 5" should "be ACGT and ACGTA respectively" in {
    val b = Array[Byte](18, 53, 16) // AC GT A$
    until(b, 4) should be("ACGT")
    until(b, 5) should be("ACGTA")
  }

  // AATAGGGTTA$ => AA TA GG GT TA $ -> use Integer.parseInt("1001", 2)
  // to convert binary string into int with the specify radix
  "substring on Array[Byte] of AATAGGGTTA$" should "correctly be sliced with given range" in {
    // 01 23 45 67 89 10
    // AA TA GG GT TA $
    val arr = Array[Byte](17, 81, 51, 53, 81, 0)

    // even_start_even_end
    subdecode(arr, 0, 2) should be("AA")
    subdecode(arr, 0, 6) should be("AATAGG")
    subdecode(arr, 2, 4) should be("TA")
    subdecode(arr, 2, 8) should be("TAGGGT")

    // even_start_odd_end
    subdecode(arr, 0, 5) should be("AATAG")
    subdecode(arr, 4, 5) should be("G")

    // odd_start_even_end
    subdecode(arr, 3, 4) should be("A")
    subdecode(arr, 1, 6) should be("ATAGG")

    // odd_start_odd_end
    subdecode(arr, 3, 5) should be("AG")
    subdecode(arr, 1, 9) should be("ATAGGGTT")

    // the wohle seq array
    subdecode(arr, 0, len(arr)) should be("AATAGGGTTA$")
    // AA AA A$
    val a = Array[Byte](17, 17, 16)
    decode(subarraybyte(a, 0, a.length * 2)) should be("AAAAA$")


    // 01 23 45 67 89 10
    // AA TA AT GA AT G$
    val b = Array[Byte](17, 81, 21, 49, 21, 48)
    subdecode(b, 1, 6) should be("ATAAT")
    subdecode(b, 3, 12) should be("AATGAATG$")
    subdecode(b, 1, len(b)) should be("ATAATGAATG$")
  }


  "funciton len" should "return the correct length of our compressed Array[Byte]" in {
    // A$
    len(Array[Byte](16)) should be(2)

    // AC TG $
    len(Array[Byte](18, 83, 0)) should be(5)

    // $
    Array[Byte](0).length should be(1)

    // AC TG G$
    len(Array[Byte](18, 83, 48)) should be(6)
  }
}