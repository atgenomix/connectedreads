package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.model.Sequence
import com.atgenomix.connectedreads.core.algorithm.SuffixArray
import com.atgenomix.connectedreads.core.model.TwoBitIndexedSeq
import com.atgenomix.connectedreads.core.rdd.KeyPartitioner4
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class TwoBitIndexedSeqSpec extends FlatSpec with Matchers {
  // A(00), C(01), G(10), T(11)
  "-81" should "be decoded to GGTT$" in {
    TwoBitIndexedSeq(seq = Array[Byte](-81), length = 4, pos=ArrayBuffer(0L))
      .fullDecode should be("GGTT$")
  }

  "-48" should "be decode to TCA$" in {
    TwoBitIndexedSeq(seq = Array[Byte](-48), length = 3, pos=ArrayBuffer(0L))
      .fullDecode should be("TCA$")
  }

  "[B" should "be converted to AACCGGC$" in {
    TwoBitIndexedSeq(seq = Array[Byte](5, Integer.parseInt("10100100", 2).toByte), length = 7, pos=ArrayBuffer(0L))
      .fullDecode should be("AACCGGC$")
  }

  "CCGGC$" should "be subsequence of AACCGGC$" in {
    val t = TwoBitIndexedSeq(Array[Byte](5, Integer.parseInt("10100100", 2).toByte), length = 7, pos=ArrayBuffer(0L))
    val u = TwoBitIndexedSeq(t.subsequence(2, 7), length = 5, pos=ArrayBuffer(0L))
    u.decode should be("CCGGC$")
  }

  "CGGC$" should "be subsequence of AACCGGC$" in {
    val t = TwoBitIndexedSeq(Array[Byte](5, Integer.parseInt("10100100", 2).toByte), length = 7, pos=ArrayBuffer(0L))
    val u = TwoBitIndexedSeq(t.subsequence(3, 8), length = 4, pos=ArrayBuffer(0L))
    u.decode should be("CGGC$")
  }

  "ttttt" should "yyyy" in {
    val kp = new KeyPartitioner4(3)
    println("@@@@@@@@ " + kp.getPartition(Array[Byte](
      4, -3, 64, 114, -127, 49, -34, 83, -1, 64, -117, 0, -72, -59, -57, 13, 29, 55, -40, -48
    )))
  }


//  "seeeeeee" should "ttttttt" in {
//    val s = "CCAAAACTGCACTAGTGCTACTAAATATATTTTATAATTTATAATTGTTATGCTGAATAAGTGGAACTCAAAAGGACTTACATTTTTAGAAGATGACTTACATTTTTCCTCCCACAACTTCCATTAAGAATTATTTTGAAG"
//    val suffix = SuffixArray.build2(s, 100L, 3, 45)
//    println(suffix._1.mkString(","))
//    for ((k, v) <- suffix._2) {
//      printf("key: %s, value: %s\n", k, v.mkString(","))
//    }
//  }


}
