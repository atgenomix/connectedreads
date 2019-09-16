package com.atgenomix.connectedreads.test

import org.scalatest.{FlatSpec, Matchers}

class SuffixExpansionSpec extends FlatSpec with Matchers {
  "suffix expansion" should "pass with a minimal suffix length constrain" in {
    def suffix(s: String, start: Int, minsf: Int): List[(String, Int)] = {
      (0 to s.length - minsf)
        .map(i => (s.substring(i) + "$", i + start))
        .toList
    }

    val results = suffix("ATCGAATTCCGG", 100, 4)
    results.foreach(println)

    results.head shouldBe("ATCGAATTCCGG$", 100)
    results.last shouldBe("CCGG$", 108)
  }
}
