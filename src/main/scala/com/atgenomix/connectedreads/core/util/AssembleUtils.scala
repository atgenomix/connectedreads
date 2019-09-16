package com.atgenomix.connectedreads.core.util

import java.nio.file.Paths

import LabelUtils.{LABEL_B, LABEL_S, LABEL_T, QUAL, RANK, RC_MASK, SEQ, START}
import SequenceUtils.reverseComplementary
import QualUtils.{decodeQual, mergeQualAndDepth}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.reflect.runtime.universe.TypeTag

object AssembleUtils {
  case class PairLen(id: Long, startId: Long, endId: Long, len: Int)
  case class PairId(inLabel: Byte, outLabel: Byte,
                    in: (Long, Long), out: (Long, Long),
                    len1: Int, jointLen: Int, len2: Int,
                    startId: Long,
                    endId: Long,
                    overlapLen1: Int, overlapLen2: Int,
                    id: Long)
  case class RankedPair(inLabel: Byte, outLabel: Byte,
                        in: (Long, Long), out: (Long, Long),
                        len1: Int, jointLen: Int, len2: Int,
                        startId: Long,
                        endId: Long,
                        overlapLen1: Int, overlapLen2: Int,
                        trigger: Boolean, multi: Boolean,
                        start: (Long, Long), rank: Int)
  case class MetaRead(id: Long, overlapLen: Int, rank: Int)
  case class MetaContig(reads: Seq[MetaRead], startId: Long, endId: Long)
  case class MetaContigWithId(reads: Seq[MetaRead], startId: Long, endId: Long, cid: Long)
  case class MetaReadWithSeq(id: Long, seq: String, qual: String, cid: Long, metaRead: MetaRead)
  case class Contig(seq: String, qual: String, id: Long, endId: Long) extends GraphUtils.VertexTrait
  case class RmDupContig(seq: String, qual: String, id: Long, endId: Long, cid: Long, rc: Byte)
  case class PairedContig(id1: Long, len1: Int, head1: String, tail1: String, id2: Long, len2: Int, head2: String, tail2: String)
  def calcLen(pair: PairId): Int = {
    pair.len1 + pair.jointLen + pair.len2 - pair.overlapLen2 - pair.overlapLen2
  }

  implicit class DataFramesEnhance(df: DataFrame)(implicit spark: SparkSession) {
    final val inId = "in._1"
    final val outId = "out._2"

    def cacheDataFrame(): DataFrame = {
      val frame = df.persist(StorageLevel.MEMORY_ONLY_SER).checkpoint()
      df.unpersist()
      frame
    }

    def rmRedundantBrokenPair()(implicit spark: SparkSession): DataFrame = {
      import GraphUtils.Pair
      import spark.implicits._
      val encoder = Encoders.product[Pair]

      val singlePairs = df.as[Pair](encoder).filter(_.in == (-1, -1))
      val multiplePairs = df.as[Pair](encoder)
        .filter(_.in != (-1, -1))
        .groupByKey(_.out._1)
        .flatMapGroups { case (_, iter) =>
          // remove redundant pairs
          // e.g. 1 -> 2 -> 3 and -1 -> 2 -> 3 should keep 1 -> 2 -> 3
          val pairs = iter.toList
          val unBrokenPairs = pairs.filter(i => i.in._1 != -1 && i.out._2 != -1)
          val headBrokenPairs = pairs.filter(i => i.in._1 == -1 && i.out._2 != -1)
            .filterNot(i => unBrokenPairs.exists(_.out._2 == i.out._2))
          val tailBrokenPairs = pairs.filter(i => i.in._1 != -1 && i.out._2 == -1)
            .filterNot(i => unBrokenPairs.exists(_.in._1 == i.in._1))

          unBrokenPairs ++ headBrokenPairs ++ tailBrokenPairs
        }
      (singlePairs union multiplePairs).toDF()
    }

    def updatePairLabel(): DataFrame = {
      // persist the dataframe prevent it from invoking monotonically_increasing_id twice
      val df2 = df.withColumn("id", monotonically_increasing_id)
        .persist(StorageLevel.MEMORY_ONLY_SER)
      df2.count()

      // get pairs which is not single vertex
      // unnecessary to update label of single vertex pairs
      val h = df2.filter(!(col("in._1") === -1 and col("in._2") === -1))
        .withColumn("edge", df2("in"))
        .select("id", "edge")
        .toDF()
      val t = df2.filter(!(col("in._1") === -1 and col("in._2") === -1))
        .withColumn("edge", df2("out"))
        .select("id", "edge")
        .toDF()

      // find pairs that should update label
      val sPair = h.join(t, h("edge") === t("edge"), "leftanti")
        .withColumn("inLabel", lit(LABEL_S))
        .select("id", "inLabel")
      val tPair = t.join(h, t("edge") === h("edge"), "leftanti")
        .withColumn("outLabel", lit(LABEL_T))
        .select("id", "outLabel")

      // update label
      val inLabelUpdated = df2.join(sPair, Seq("id"), "left")
        .withColumn("updatedInLabel", coalesce(sPair("inLabel"), df2("inLabel")))
        .drop("inLabel")
      val outLabelUpdated = inLabelUpdated.join(tPair, Seq("id"), "left")
        .withColumn("updatedOutLabel", coalesce(tPair("outLabel"), inLabelUpdated("outLabel")))
        .drop("outLabel")

      outLabelUpdated.select(Array(col("updatedInLabel").as("inLabel"),
        col("updatedOutLabel").as("outLabel")) ++
        df.columns.drop(2).map(df(_)): _*)
    }

    // only fetch longest pair if there are multiple pairs which have same startId/endId
    def removeRedundantPairs()(implicit spark: SparkSession): DataFrame = {
      val brokenPairs = df.filter((col(inId) === -1 or col(outId) === -1) and
        col("inLabel") === LABEL_S and
        col("outLabel") === LABEL_T)
        .withColumn("id", monotonically_increasing_id)
        .persist(StorageLevel.MEMORY_ONLY_SER)
      brokenPairs.count()
      val remain = df.except(brokenPairs.drop("id"))

      import spark.implicits._
      val idEncoder = Encoders.product[PairId]
      val lenEncoder = Encoders.product[PairLen]
      val lenPairs = brokenPairs.as[PairId](idEncoder)
        .map { p =>
          PairLen(p.id, p.startId, p.endId, calcLen(p))
        }
      val longestPairs = lenPairs.as[PairLen](lenEncoder)
        .groupByKey(_.startId)
        .mapGroups { case (_, pairs) => pairs.maxBy(_.len) }
      val longestPairs2 = longestPairs.groupByKey(_.endId)
        .mapGroups { case (_, pairs) => pairs.maxBy(_.len) }
        .select("id")

      remain union brokenPairs.join(longestPairs2, Seq("id"), "left_semi").drop("id")
    }

    def connectReads(upperBound: Int): DataFrame = {
      @tailrec
      def connectReadsAux(current: DataFrame, reads: DataFrame, accum: DataFrame, iter: Int): DataFrame = {
        if (iter >= upperBound || current.count() == 0) {
          accum
        } else {
          val result = reads.as("reads")
            .join(current.as("current"), col("current.out") === col("reads.in"), "right")
          val traversed = result
            .select(reads.columns.dropRight(2).map(reads(_)) :+ current(START): _*)
            .withColumn(RANK, lit(iter))
          traversed.persist(StorageLevel.MEMORY_ONLY_SER).checkpoint()

          connectReadsAux(traversed.na.drop.filter(col("outLabel") =!= LABEL_T),
            reads,
            accum union traversed.na.drop,
            iter + 1
          )
        }
      }

      val sbPairs = df.filter(col("inLabel") === LABEL_S and col("outLabel") === LABEL_B)
        .withColumn(START, col("in"))
        .withColumn(RANK, lit(0))
      val bPairs = df.filter(col("inLabel") === LABEL_B)
        .withColumn(START, lit(Array(-1L, -1L)))
        .withColumn(RANK, lit(0))
      val triplets = df.filter(col("inLabel") === LABEL_S and col("outLabel") === LABEL_T)
      val headSTPairs = triplets.filter(col(outId) === -1)
        .withColumn(START, col("in"))
        .withColumn(RANK, lit(0))
      val tailSTPairs = triplets.filter(col(inId) === -1 and col("in._2") =!= -1)
        .withColumn(START, col("out"))
        .withColumn(RANK, lit(0))
      val genSingleVertexStart = udf((outId: Long) => (outId, outId))
      val singleVertex = triplets.filter(col("in._1") === -1 and col("in._2") === -1)
        .withColumn(START, genSingleVertexStart(col(outId)))
        .withColumn(RANK, lit(0))
      val normalSTPairs = triplets.filter(col(inId) =!= -1 and col("in._2") =!= -1 and col(outId) =!= -1)
        .withColumn(START, col("in"))
        .withColumn(RANK, lit(0))
      val stPairs = (headSTPairs union tailSTPairs union singleVertex union normalSTPairs).distinct()

      connectReadsAux(sbPairs,
        bPairs,
        stPairs union sbPairs,
        1)
    }

    def rmRedundantSingleVertex()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val encoder = Encoders.product[RankedPair]
      df.as[RankedPair](encoder)
        .groupByKey(_.startId)
        .flatMapGroups { case (_, iter) =>
          val pairs = iter.toList
          if (pairs.length == 1) pairs else pairs.filter(p => p.startId != p.endId)
        }
        .groupByKey(_.endId)
        .flatMapGroups { case (_, iter) =>
          val pairs = iter.toList
          if (pairs.length == 1) pairs else pairs.filter(p => p.startId != p.endId)
        }
        .toDF()
    }

    def genContigs(one2oneSeq: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val encoder = Encoders.product[RankedPair]
      val contigWithIdEncoder = Encoders.product[MetaContigWithId]
      val seqEncoder = Encoders.product[MetaReadWithSeq]

      val metaContigWithId = df.as[RankedPair](encoder)
        .groupByKey(_.start)
        .flatMapGroups { case (_, pairs) =>
          val sortedPairs = pairs.toArray.sortBy(_.rank)
          breakByHaploAware(sortedPairs)
        }
        .toDF()
        .rmSubContigs()
        .withColumn("cid", monotonically_increasing_id)
        .as[MetaContigWithId](contigWithIdEncoder)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      metaContigWithId.count()

      val contigInfo = metaContigWithId
        .map { i => (i.cid, i.startId, i.endId) }
        .toDF("cid", "startId", "endId")

      val metaReads = metaContigWithId
        .flatMap { meta =>
          val cid = meta.cid
          meta.reads.map(r => cid -> r)
        }.toDF("cid", "metaRead")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      metaReads.count()

      val contigs = one2oneSeq.join(metaReads, one2oneSeq("id") === metaReads("metaRead.id"))
        .as[MetaReadWithSeq](seqEncoder)
        .groupByKey(_.cid)
        .mapGroups { case (cid, iter) =>
          val result = iter.map(i => (i.metaRead, i.seq, i.qual))
            .toArray
            .sortBy(_._1.rank)
            .foldLeft((new StringBuilder(""), new StringBuilder(""))){ case ((seqResult, qualResult), (metaRead, seq, qual)) =>
              seqResult ++= seq.substring(metaRead.overlapLen)
              if (qualResult.isEmpty) {
                qualResult ++= qual.substring(metaRead.overlapLen)
                seqResult -> qualResult
              } else {
                val (q1, overlap1) = qualResult.splitAt(qualResult.length - metaRead.overlapLen)
                val (overlap2, q2) = qual.splitAt(metaRead.overlapLen)
                val decoded1 = decodeQual(overlap1.toString)
                val decoded2 = decodeQual(overlap2)
                val merged = mergeQualAndDepth(List(decoded1, decoded2))

                seqResult -> (q1 ++= merged ++= q2)
              }
            }
          (cid, result._1.toString, result._2.toString)
        }
        .toDF("cid", SEQ, QUAL)

      contigs.join(contigInfo, Seq("cid"))
        .select(col(SEQ), col(QUAL), col("startId").as("id"), col("endId"))
    }

    def rmSubContigs()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val encoder = Encoders.product[MetaContig]

      val collectContigs = (metaContigs: Iterator[MetaContig]) =>
          metaContigs.toArray.sortBy(_.reads.size)(Ordering.Int.reverse)
            .foldLeft(List[MetaContig]()){ case (r, contig) =>
              val isSubContig = r.map(_.reads.map(_.id))
                .exists{ i => i.intersect(contig.reads.map(_.id)).size == contig.reads.size }
              if (r.isEmpty || !isSubContig) contig :: r else  r
            }

      df.as[MetaContig](encoder)
        .groupByKey(_.startId)
        .flatMapGroups { case (_, metaContigs) => collectContigs(metaContigs) }
        .groupByKey(_.endId)
        .flatMapGroups { case (_, metaContigs) => collectContigs(metaContigs) }
        .toDF
    }

    def removeDuplicated()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val encoder = Encoders.product[RmDupContig]

      df.withColumn("cid", monotonically_increasing_id)
        .withColumn("rc", lit(0x0))
        .as[RmDupContig](encoder)
        .flatMap { c =>
          // unnecessary to generate the reverse of qual, because rc contigs will be filtered out at the end
          val rcContig = RmDupContig(reverseComplementary(c.seq), "", c.endId ^ RC_MASK, c.id ^ RC_MASK, c.cid, 0x1)
          List(c, rcContig)
        }
        .groupByKey(_.seq)
        .mapGroups { case (_, contigs) => contigs.maxBy(_.cid) }
        .filter(_.rc == 0x0)
        .drop("cid", "rc")
        .toDF()
    }

    def adamOutput[T <: GraphUtils.VertexTrait: TypeTag](path: String)(implicit spark: SparkSession): Unit = {
      val encoder = Encoders.product[T]

      val records = df.as[T](encoder)
        .rdd
        .map { row =>
          val seq = row.seq
          val qual = row.qual
          val startId = row.id
          val endId = row.endId
          val record = AlignmentRecord.newBuilder
            .setReadName(s"CONTIG-$startId-$endId")
            .setSequence(seq)
            .setQual(qual)
            .setReadPaired(false)
            .build
          record
        }

      AlignmentRecordRDD(records, SequenceDictionary.empty, RecordGroupDictionary.empty, Seq.empty).saveAsParquet(
        Paths.get(path).toString,
        128 * 1024 * 1024,
        1 * 1024 * 1024,
        CompressionCodecName.SNAPPY)
    }
  }

  def breakByHaploAware(pairs: Array[RankedPair]): List[MetaContig] = {
    val (result, pairQueue, _) = pairs.foldLeft((List[MetaContig](), Queue[RankedPair](), false)){ case ((metaContigs, accum, trigger), p) =>
        if (trigger && p.multi) {
          val headBrokenP = p.copy(in = (-1, p.out._1), len1 = 0, overlapLen1 = 0, start = p.out, startId = p.out._1)
          val tailBrokenP = p.copy(out = (p.out._1, -1), len2 = 0, overlapLen2 = 0, endId = p.out._1)
          (genMetaContig((accum :+ tailBrokenP).toList) :: metaContigs, Queue[RankedPair](headBrokenP), false)
        } else {
          val t = if (p.trigger) true else trigger
          (metaContigs, accum :+ p, t)
        }
    }

    genMetaContig(pairQueue.toList) :: result
  }

  def genMetaContig(pairs: List[RankedPair]): MetaContig = {
    val firstPair = pairs.head
    val initReads = List(MetaRead(firstPair.in._1, 0, 0),
      MetaRead(firstPair.out._1, firstPair.overlapLen1, 1),
      MetaRead(firstPair.out._2, firstPair.overlapLen2, 2))
    val reads = pairs.tail
      .foldLeft(initReads){ case (r, pair) => r :+ MetaRead(pair.out._2, pair.overlapLen2, pair.rank + 2) }
      .filter(_.id != -1)
    val startId = reads.head.id
    val endId = reads.last.id

    MetaContig(reads, startId, endId)
  }
}
