package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.model.TwoBitSufSeq
import com.atgenomix.connectedreads.core.rdd.{CommonPrefix, KeyPartitioner4, Suffixes}
import com.atgenomix.connectedreads.core.util.SuffixUtils.suffix
import com.atgenomix.connectedreads.test.base.SparkSessionSpec
import org.scalatest.{FlatSpec, Matchers}

class NewSeqMLSpec extends FlatSpec with SparkSessionSpec with Matchers {
  "The testing data" should "generate 20 GenomeSequences and output unnecessary records that are marked not leaves " in {
    val _spark = spark
    import _spark.implicits._
    val pl = 3.toShort
    val minsfx = 10
    val suffixes = spark.sparkContext.parallelize(Array(
      ("AATACTACCA", 1L),
      ("AATACTACCG", 2L),
      ("AATACTACCTT", 3L),
      ("AATACTCCAG", 4L),
      ("AATAGGGACA", 5L),
      ("AATAGGGACG", 6L),
      ("AATAGGGACT", 7L),
      ("AATAGGGCGT", 8L),
      ("AATAGGGTTA", 9L),
      ("AATAGGGTTC", 10L),
      ("AATAGGGTTT", 11L),
      ("AATATTTAGA", 12L),
      ("AATATTTAGC", 13L),
      ("AATATTTAGG", 14L),
      ("AATATTTTCA", 15L),
      ("AATATTTTCC", 16L),
      ("AATATTTTCT", 17L)
    ))
      .flatMap(x => suffix(x._1, x._2, pl, minsfx))
      .partitionBy(new KeyPartitioner4(6,3)).toDS()
      .flatMap(x => x._2._2.map(offset => TwoBitSufSeq(seq = x._1, offset = offset, length = (x._2._3 - offset).toShort,
        idx = 0, pos = x._2._1 + offset.toLong)))
      .mapPartitions(_.toList.sorted.iterator) // 1st sort

    val cp = suffixes.mapPartitions(iter => CommonPrefix.run(iter, pl))
    val gs = cp.mapPartitions(_.toList.sorted.iterator).mapPartitions(Suffixes.index(_, pl))

    val gss = gs.collect
    gss.foreach(x => println("## " + x.toString))
    println("### " + gss.length)

    gss.length should be(26) // isolated item => seq=ATACTACCTT$
  }

  "xx" should "oo" in {
    val data = spark.createDataFrame(List(("A",1), ("B",2), ("C",3))).toDF("id", "num")
    println("@@@@@" + data.filter(data("id") === "A").count)
    println("@@@@@" + data.filter(data("id") =!= "A").count)
  }
}
