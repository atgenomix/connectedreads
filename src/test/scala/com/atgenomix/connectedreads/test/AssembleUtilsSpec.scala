package com.atgenomix.connectedreads.test

import com.atgenomix.connectedreads.core.util.AssembleUtils._
import com.atgenomix.connectedreads.core.util.GraphUtils.Pair
import com.atgenomix.connectedreads.core.util.LabelUtils.{LABEL_B, LABEL_S, LABEL_T}
import com.atgenomix.connectedreads.test.base.SparkSessionSpec
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class AssembleUtilsSpec extends FlatSpec with Matchers with SparkSessionSpec {
  case class Contig(seq: String)
  "updateLabel()" should "update pair label" in {
    val pairs = spark.sqlContext
      .createDataFrame(List(
        Pair(LABEL_B, LABEL_B, (5, 2), (2, 6), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (2, 3), (3, 4), 0, 0, 0,  0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (3, 4), (4, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (1, -1), (-1, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_B, (-1, 2), (2, 3), 0, 0, 0, 0, 0, 0, 0, false, false)
      ))
    val ans = spark.sqlContext
      .createDataFrame(List(
        Pair(LABEL_S, LABEL_T, (5, 2), (2, 6), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (2, 3), (3, 4), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (3, 4), (4, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_T, (1, -1), (-1, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_B, (-1, 2), (2, 3), 0, 0, 0, 0, 0, 0, 0, false, false)
      ))

    implicit val session = spark
    pairs.updatePairLabel().sort(col("in")).collect() should be(ans.sort(col("in")).collect())
  }

  "updateLabel()" should "update pair label(oval shape graph)" in {
    val pairs = spark.sqlContext
      .createDataFrame(List(
        Pair(LABEL_B, LABEL_B, (1, 2), (2, 4), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (1, 3), (3, 4), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (2, 4), (4, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (3, 4), (4, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (4, 5), (5, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_B, (4, 6), (6, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (5, 7), (7, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (6, 7), (7, -1), 0, 0, 0, 0, 0, 0, 0, false, false)
      ))
    val ans = spark.sqlContext
      .createDataFrame(List(
        Pair(LABEL_S, LABEL_B, (1, 2), (2, 4), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_B, (1, 3), (3, 4), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (2, 4), (4, -1),0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (3, 4), (4, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_B, (4, 5), (5, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_S, LABEL_B, (4, 6), (6, 7), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (5, 7), (7, -1), 0, 0, 0, 0, 0, 0, 0, false, false),
        Pair(LABEL_B, LABEL_T, (6, 7), (7, -1), 0, 0, 0, 0, 0, 0, 0, false, false)
      ))

    implicit val session = spark
    pairs.updatePairLabel().sort(col("in")).collect() should be(ans.sort(col("in")).collect())
  }

  "connectReads()" should "" in {
    val pairs = spark.sqlContext
      .createDataFrame(List(
        RankedPair(LABEL_S, LABEL_T, (5, 2), (2, 6), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, -1), 0),
        RankedPair(LABEL_B, LABEL_B, (2, 3), (3, 4), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, -1), 0),
        RankedPair(LABEL_B, LABEL_T, (3, 4), (4, 7), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, -1), 0),
        RankedPair(LABEL_S, LABEL_T, (1, 2), (2, -1), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, -1), 0),
        RankedPair(LABEL_S, LABEL_B, (-1, 2), (2, 3), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, -1), 0)
      ))
    val ans = spark.sqlContext
      .createDataFrame(List(
        RankedPair(LABEL_S, LABEL_T, (5, 2), (2, 6), 0, 0, 0, 0, 0, 0, 0, false, false, (5, 2), 0),
        RankedPair(LABEL_B, LABEL_B, (2, 3), (3, 4), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, 2), 1),
        RankedPair(LABEL_B, LABEL_T, (3, 4), (4, 7), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, 2), 2),
        RankedPair(LABEL_S, LABEL_T, (1, 2), (2, -1), 0, 0, 0, 0, 0, 0, 0, false, false, (1, 2), 0),
        RankedPair(LABEL_S, LABEL_B, (-1, 2), (2, 3), 0, 0, 0, 0, 0, 0, 0, false, false, (-1, 2), 0)
      ))

    implicit val session = spark
    spark.sparkContext.setCheckpointDir("/tmp/test-checkpoint")
    pairs.connectReads(50).orderBy("start", "rank").collect() should be(
      ans.orderBy("start", "rank").collect())
  }
}
