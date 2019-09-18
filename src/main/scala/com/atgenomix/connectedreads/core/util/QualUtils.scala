package com.atgenomix.connectedreads.core.util

object QualUtils {
  val qualMin = 33
  val qualMax = 74
  val qLevel: Array[Int] = Array[Int](
33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, // !
    47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47,  // /
    61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61)  // =

  def decodeQual(encodedQual: String): (String, List[Int]) = {
    val (qual, depth) = encodedQual.map { c =>
      if (c.toInt > qualMax) {
        61 -> 14
      } else {
        val q = qualMin + ((c.toInt - qualMin) / 14) * 14
        // need + 1 because the encode function is: qLevel(q - qualMin) + encodedDepth - 1
        // the depth does not include itself
        val d = (c.toInt - qualMin) % 14 + 1
        q -> d
      }
    }.unzip

    qual.map(_.toChar).mkString -> depth.toList
  }

  def mergeQualAndDepth(input: List[(String, List[Int])]): String = {
    val (quals, depths) = input.unzip
    val len = depths.head.size
    val newDepth = calcDepth(depths, len - 1, List())
    val bestQual = chooseBestQual(quals.map(_.toList), len - 1, List())
    (newDepth zip bestQual).map(i => (i._1 + i._2 - 1).toChar).mkString
  }

  def calcDepth(depths: List[List[Int]], idx: Int, accum: List[Int]): List[Int] = {
    if (idx < 0) {
      accum
    } else {
      val totalDepth = depths.map(d => d(idx)).sum
      calcDepth(depths, idx - 1, totalDepth :: accum)
    }
  }

  def chooseBestQual(chars: List[List[Char]], idx: Int, accum: List[Char]): List[Int] = {
    if (idx < 0) {
      accum.map(_.toInt)
    } else {
      val bestQual = chars.map(c => c(idx)).max
      chooseBestQual(chars, idx - 1, bestQual :: accum)
    }
  }
}
