package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.util.ArrayByteUtils._
import org.scalatest.{FlatSpec, Matchers}


class NewArrayByteUtilsSpec extends FlatSpec with Matchers {
  "The following sequences" should "be encoded correctly" in {
    val s1 = encode("ATCGACCCT")
    s1._1 should contain theSameElementsInOrderAs Array[Byte](54, 21, -64)
    s1._2 should be(10)

    val s2 = encode("ATCGACCCT", 4)
    s2._1 should contain theSameElementsInOrderAs Array[Byte](21, -64)
    s2._2 should be(6)

    val s3 = encode("ACT")
    s3._1 should contain theSameElementsInOrderAs Array[Byte](28)
    s3._2 should be(4)

    val s4 = encode("GTC", 1)
    s4._1 should contain theSameElementsInOrderAs Array[Byte](-48)
    s4._2 should be(3)

    val s5 = encode("CCGTAT")
    s5._1 should contain theSameElementsInOrderAs Array[Byte](91, 48)
    s5._2 should be(7)
    
    val s60 = encode("AATAATACCTT", 0)
    val s61 = encode("AATAATACCTT", 4)
    val s62 = encode("AATAATACCTT", 10)
    s61._1 should contain theSameElementsInOrderAs s60._1.drop(1)
    s62._1 should contain theSameElementsInOrderAs Array[Byte](-64)
  }
}
