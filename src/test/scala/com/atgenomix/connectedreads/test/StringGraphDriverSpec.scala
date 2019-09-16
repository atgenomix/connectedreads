package com.atgenomix.connectedreads.test

import org.scalatest.{FlatSpec, Matchers}
import com.atgenomix.connectedreads.core.util.ArrayByteUtils._

class StringGraphDriverSpec extends FlatSpec with Matchers {
  "ACTTGGCCAGA" should "have reverse complementary sequence TCTGGCCAAGT" in {
    def reverseComplementary(s: String): String = {
      val complementary = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C')
      var i = 0
      var j = s.length - 1
      val c = s.toCharArray

      while (i < j) {
        val temp = c(i)
        c(i) = complementary(c(j))
        c(j) = complementary(temp)
        i = i + 1
        j = j - 1
      }

      if (s.length % 2 != 0) c(i) = complementary(c(i))

      String.valueOf(c)
    }

    reverseComplementary("ACTTGGCCAGA") should be("TCTGGCCAAGT")
  }


//  "ffff" should "tttttt" in {
//    // 測試 encode
//    import com.atgenomix.connectedreads.core.util.util.ArrayByteUtils._
//    //import .encode
//    val s = encode("AAACACAAC", 0)
//    println(s._1.mkString(",") + " >>>>>>>> " + s._2  )
//
//    val t = subarraybyte(s._1, 3, 8)
//    println("@@@@@@@@@ " + t.mkString(","))
//  }

}
