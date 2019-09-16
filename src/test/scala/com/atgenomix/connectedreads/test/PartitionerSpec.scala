package com.atgenomix.connectedreads.test

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PartitionerSpec extends FlatSpec with Matchers {
  "4 power 3" should "generates 64 partitions" in {
    val bases = Array("A", "C", "G", "T")
    val stack = mutable.Stack[String]()
    var buf = new ListBuffer[String]()
    val power = 3

    val base4 = (s: String) => {
      val char = s.toArray
      for (i <- char.indices) {
        char(i) = char(i) match {
          case 'A' => '0'
          case 'C' => '1'
          case 'G' => '2'
          case 'T' => '3'
        }
      }
      char.mkString("")
    }

    for (i <- 0 until power - 1) {
      if (i == 0) stack.push("A", "C", "G", "T")

      while (stack.nonEmpty) {
        val next = stack.pop()
        for (b <- bases) {
          buf += next + b
        }
      }
      // persist the intermediate results
      if (i < power - 1 - 1) {
        stack.pushAll(buf)
        buf.clear()
      }
    }

    buf.foreach(x => {
      val b4 = base4(x)
      println(x + " >> " + b4 + " >> " + Integer.parseInt(b4, 4))
    })

    val results = buf.toList
    results.contains("ACC") should be(true)
    results.length should be(64)

  }

  "ACG" should "be converted into 7" in {
    val s = "ACG"
    var n: Double = 0d
    for (i <- s.indices) {
      val b: Short = s(i) match {
        case 'A' => 0
        case 'C' => 1
        case 'G' => 2
        case 'N' => 3
        case 'T' => 4
        case _ => throw new IllegalArgumentException(s(i).toString)
      }
      n += b * scala.math.pow(5, s.length - i - 1)
    }
    n.toInt should be(7)
  }
}
