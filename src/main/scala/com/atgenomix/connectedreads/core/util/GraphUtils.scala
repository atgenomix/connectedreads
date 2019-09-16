package com.atgenomix.connectedreads.core.util

import bloomfilter.mutable.BloomFilter
import com.atgenomix.connectedreads.core.util.AssembleUtils._
import com.atgenomix.connectedreads.core.util.LabelUtils._
import com.atgenomix.connectedreads.core.util.QualUtils.{decodeQual, mergeQualAndDepth}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.graphframes._
import org.graphframes.GraphFrame.{DST, ID, SRC}
import org.graphframes.lib.AggregateMessages

import scala.annotation.tailrec
import java.util.UUID.randomUUID

import com.atgenomix.connectedreads.core.util.LabelUtils.{AssemblyEdge, AssemblyVertex, NormalizedEdge}

object GraphUtils {
  final val BROKEN_HEAD: Byte = 0x1
  final val BROKEN_TAIL: Byte = 0x2
  final val BROKEN_BOTH: Byte = 0x3
  trait VertexTrait extends Product { val seq: String; val qual: String; val id: Long; val endId: Long}
  case class RankedVertex(id: Long, label: Byte, start: Long, overlapLen: Int, rank: Int, firstSeq: String, firstReads: Seq[Long], qual: String)
  case class OneToOneVertex(reads: Seq[Long], seq: String, qual: String, label: Byte, id: Long, endId: Long, inDegree: Int, outDegree: Int) extends VertexTrait
  case class OneToOneEdge(src: Long, dst: Long, overlapLen: Int)
  case class OneToOneTriplet(src: OneToOneVertex, edge: OneToOneEdge, dst: OneToOneVertex)
  case class SlimOneToOneVertex(reads: Seq[Long], len: Int, label: Byte, id: Long, endId: Long)
  case class ReadTriplet(reads1: Seq[Long], id1: Long, len1: Int, label1: Byte,
                         overlapLen1: Int,
                         jointReads: Seq[Long], jointId: Long, jointLen: Int,
                         overlapLen2: Int,
                         reads2: Seq[Long],id2: Long, len2: Int, label2: Byte)
  case class Pair(inLabel: Byte, outLabel: Byte,
                  in: (Long, Long), out: (Long, Long),
                  len1: Int, jointLen: Int, len2: Int,
                  startId: Long,
                  endId: Long,
                  overlapLen1: Int, overlapLen2: Int,
                  trigger: Boolean,
                  multi: Boolean)
  object Pair{
    def toPairs(score: Long, triplet: ReadTriplet, multi: Boolean): List[Pair] = {
      List(Pair(inLabel = LABEL_B,  // may be S or B, determined by updatePairLabel()
        outLabel = LABEL_B, // may be B or T, determined by updatePairLabel()
        in = (triplet.id1, triplet.jointId),
        out = (triplet.jointId, triplet.id2),
        len1 = triplet.len1,
        jointLen = triplet.jointLen,
        len2 = triplet.len2,
        startId = triplet.id1,        // for removeRedundantPairs()
        endId = triplet.id2,  // for removeRedundantPairs()
        overlapLen1 = triplet.overlapLen1,
        overlapLen2 = triplet.overlapLen2,
        trigger = false,
        multi = multi))
    }

    // remove unnecessary pair
    //
    //          / -\- C --- \
    // A ---> B               o
    //          \ -\- D --- /
    //
    // if ReadTriplet ABC and ABD are broken, then should only exist Pair AB, Pair BC and Pair BD
    // should not exist Pair -1B(single vertex pair)
    def toBrokenPairs(brokenType: Byte, score: Long, triplet: ReadTriplet, multi: Boolean): List[Pair] = {
      brokenType match {
        case BROKEN_TAIL => brokenTail(score, triplet, multi: Boolean)
        case BROKEN_HEAD => brokenHead(score, triplet, multi: Boolean)
        case BROKEN_BOTH => brokenBoth(score, triplet, multi: Boolean)
        case _ => throw new IllegalArgumentException("Wrong broken type")
      }
    }

    private def brokenTail(score: Long, triplet: ReadTriplet, multi: Boolean): List[Pair] = {
      val pair = Some(Pair(inLabel = LABEL_B, // may be S or B, determined by updatePairLabel()
        outLabel = LABEL_T,
        in = (triplet.id1, triplet.jointId),
        out = (triplet.jointId, -1),
        len1 = triplet.len1,
        jointLen = triplet.jointLen,
        len2 = 0,
        startId = triplet.id1, // for removeRedundantPairs()
        endId = triplet.jointId, // for removeRedundantPairs()
        overlapLen1 = triplet.overlapLen1,
        overlapLen2 = 0,
        trigger = false,
        multi = multi))
      val t = if (triplet.label2 == LABEL_T || triplet.label2 == LABEL_PT) {
        Some(Pair(inLabel = LABEL_S,
          outLabel = LABEL_T,
          // denote ST pair with single vertex by in:(-1, -1), out: (-1, id)
          in = (-1, -1),
          out = (-1, triplet.id2),
          len1 = 0,
          jointLen = 0,
          len2 = triplet.len2,
          startId = triplet.id2, // for removeRedundantPairs()
          endId = triplet.id2, // for removeRedundantPairs()
          overlapLen1 = 0,
          overlapLen2 = 0,
          trigger = false,
          multi = multi))
      } else {
        None
      }
      List(pair, t).flatten
    }

    private def brokenHead(score: Long, triplet: ReadTriplet, multi: Boolean): List[Pair] = {
      val pair = Some(
        Pair(inLabel = LABEL_S,
          outLabel = LABEL_B, // may be B or T, determined by updatePairLabel()
          in = (-1, triplet.jointId),
          out = (triplet.jointId, triplet.id2),
          len1 = 0,
          jointLen = triplet.jointLen,
          len2 = triplet.len2,
          startId = triplet.jointId, // for removeRedundantPairs()
          endId = triplet.id2, // for removeRedundantPairs()
          overlapLen1 = 0,
          overlapLen2 = triplet.overlapLen2,
          trigger = false,
          multi = multi))
      val h = if (triplet.label1 == LABEL_S || triplet.label1 == LABEL_PS) {
        Some(Pair(inLabel = LABEL_S,
          outLabel = LABEL_T,
          // denote ST pair with single vertex by in:(-1, -1), out: (-1, id)
          in = (-1, -1),
          out = (-1, triplet.id1),
          len1 = 0,
          jointLen = 0,
          len2 = triplet.len1,
          startId = triplet.id1, // for removeRedundantPairs()
          endId = triplet.id1, // for removeRedundantPairs()
          overlapLen1 = 0,
          overlapLen2 = 0,
          trigger = false,
          multi = multi))
      } else {
        None
      }
      List(pair, h).flatten
    }

    private def brokenBoth(score: Long, triplet: ReadTriplet, multi: Boolean): List[Pair] = {
      val j = Pair(inLabel = LABEL_S,
        outLabel = LABEL_T,
        // denote ST pair with single vertex by in:(-1, -1), out: (-1, id)
        in = (-1, -1),
        out = (-1, triplet.jointId),
        len1 = 0,
        jointLen = 0,
        len2 = triplet.jointLen,
        startId = triplet.jointId, // for removeRedundantPairs()
        endId = triplet.jointId, // for removeRedundantPairs()
        overlapLen1 = 0,
        overlapLen2 = 0,
        trigger = false,
        multi = multi)
      val h = Pair(inLabel = LABEL_S,
        outLabel = LABEL_T,
        // denote ST pair with single vertex by in:(-1, -1), out: (-1, id)
        in = (-1, -1),
        out = (-1, triplet.id1),
        len1 = 0,
        jointLen = 0,
        len2 = triplet.len1,
        startId = triplet.id1, // for removeRedundantPairs()
        endId = triplet.id1, // for removeRedundantPairs()
        overlapLen1 = 0,
        overlapLen2 = 0,
        trigger = false,
        multi = multi)
      val t = Pair(inLabel = LABEL_S,
        outLabel = LABEL_T,
        // denote ST pair with single vertex by in:(-1, -1), out: (-1, id)
        in = (-1, -1),
        out = (-1, triplet.id2),
        len1 = 0,
        jointLen = 0,
        len2 = triplet.len2,
        startId = triplet.id2, // for removeRedundantPairs()
        endId = triplet.id2, // for removeRedundantPairs()
        overlapLen1 = 0,
        overlapLen2 = 0,
        trigger = false,
        multi = multi)
      List(j, h, t)
    }
  }

  // vertex output: (seq: String, qual: String, id: Long, endId: Long)
  implicit class GraphFrameEnhance(graph: GraphFrame)(implicit spark: SparkSession) {
    def getIsolatedVertices(): DataFrame = {
      val endIdUdf = udf((reads: Seq[Long]) => reads.last)
      val e1 = graph.edges.withColumn(ID, explode(array(col(SRC), col(DST))))
      val v = graph.vertices
        .join(e1, Seq(ID), "left_anti")
      val isolatedV = if (v.columns.contains("reads")) {
        v.withColumn("endId", endIdUdf(col("reads")))
          .select(col(SEQ), col(QUAL), col(ID), col("endId"))
      } else {
        v.select(col(SEQ), col(QUAL), col(ID), col(ID).as("endId"))
      }

      isolatedV.cacheDataFrame()
    }

    def expandGraph(): GraphFrame = {
      import SequenceUtils.reverseComplementary
      import spark.implicits._
      val vertexEncoder = Encoders.product[AssemblyVertex]
      val edgeEncoder = Encoders.product[NormalizedEdge]

      val vertices = graph.vertices.as[AssemblyVertex](vertexEncoder).flatMap { vertex =>
        val rc = AssemblyVertex(vertex.id + RC_MASK, reverseComplementary(vertex.seq), vertex.qual.reverse)
        List(vertex, rc)
      }

      val updatedAndNewEdges = graph.edges.as[NormalizedEdge](edgeEncoder).flatMap { edge =>
        val modifiedEdge = modifyEdgeVertexID(edge)
        val reverseEdge = genReverseEdge(modifiedEdge)

        List(modifiedEdge, reverseEdge)
      }

      GraphFrame(
        vertices.toDF(),
        updatedAndNewEdges.toDF()
      )
    }

    // vertex output: (id: Long, inDegree: Int, outDegree: Int)
    def degree(): GraphFrame = {
      val degree = graph.inDegrees
        .join(graph.outDegrees, Seq(ID), "full")
        .na
        .fill(0, Seq(IN, OUT))

      GraphFrame(degree, graph.edges)
    }

    def profilingDegree(profiling: Boolean): GraphFrame = {
      if (!profiling)
        return graph

      val record = Array.ofDim[Long](4, 4)
      val v = graph.vertices.persist(StorageLevel.MEMORY_ONLY_SER)
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          val c = v.filter(s"inDegree = $i and outDegree = $j").count
          record(i)(j) = c
        }
        val c2 = v.filter(s"inDegree = $i and outDegree >= 3").count
        record(i)(3) = c2
      }
      for (i <- 0 to 2) {
        val c = v.filter(s"inDegree >= 3 and outDegree = $i").count
        record(3)(i) = c
      }
      val c = v.filter(s"inDegree >= 3 and outDegree >= 3").count
      record(3)(3) = c

      println("degree: ")
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          val c = record(i)(j)
          printf("%12.0f\t", c.toDouble)
        }
        val c2 = record(i)(3)
        printf("%12.0f\n", c2.toDouble)
      }
      for (i <- 0 to 2) {
        val c = record(3)(i)
        printf("%12.0f\t", c.toDouble)
      }
      val d = record(3)(3)
      printf("%12.0f\n", d.toDouble)

      println("degree percentage: ")
      val total = record.foldLeft(0L){ case (r, c) => r + c.sum }
       for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          val c = record(i)(j)
          printf("%1.5f\t", c.toDouble / total)
        }
        val c2 = record(i)(3)
        printf("%1.5f\n", c2.toDouble / total)
      }
      for (i <- 0 to 2) {
        val c = record(3)(i)
        printf("%1.5f\t", c.toDouble / total)
      }
      val e = record(3)(3)
      printf("%1.5f\t", e.toDouble / total)

      v.unpersist()
      graph
    }

    //  the reason to make in-degree 0 out-degree 2 as PS
    //  if it's S, consider the following situation:
    //    B -> B ->
    //   /
    //  S
    //   \
    //    B -> B ->
    //
    //  the startIds of the 2 branch will be the same, then it will only assemble one contig but should be 2
    //
    // vertex output: (id: Long, inDegree: Int, outDegree: Int, label: Byte)
    def label(): GraphFrame = {
      val label = (inDegree: Int, outDegree: Int) => (inDegree, outDegree) match {
        case (0, 1) => LABEL_S
        case (0, i) if i >= 2 => LABEL_PS
        case (1, 0) => LABEL_T
        case (1, 1) => LABEL_B
        case (i, 0) if i >= 2 => LABEL_PT
        case (0, 0) => throw new IllegalArgumentException("Isolated vertex in graph.")
        case _ => LABEL_PT_PS
      }
      val labelUDF = udf(label)

      val v = graph.vertices.withColumn(LABEL, labelUDF(col(IN), col(OUT)))
      GraphFrame(v, graph.edges)
    }

    // 1. PS will propagate S to its dst and make itself to T
    // 2. PT will propagate T to its src and make itself to PT
    // 3. PT/PS will propagate S to its dst and T to its src and make itself to PT
    // 4. N-to-1 vertex should be labeled as S (to do so due to N-to-1 and 1-to-N can be merged if they are adjacent
    // 5. 1-to-N should be labeled as T
    // vertex output: (id: Long, label: Byte)
    def propagation(): GraphFrame = {
      val AM = AggregateMessages

      val pickLabelUDF = udf((labels: Seq[Byte]) => {
        if (labels.contains(LABEL_PT_PS))
          LABEL_PT_PS
        else if (labels.contains(LABEL_PT))
          LABEL_PT
        else if (labels.contains(LABEL_T))
          LABEL_T
        else if (labels.contains(LABEL_S))
          LABEL_S
        else
          LABEL_B
      })

      // N-to-1 should be labeled as S
      // 1-to-N should be labeled as T
      val modifyPTPSUDF = udf((label: Byte, in: Int, out: Int) =>
        if (in >= 2 && out == 1)
          LABEL_S
        else if (in == 1 && out >= 2)
          LABEL_T
        else
          label
      )

      // PS and PT/PS propagate to dst
      val msgToDst: Column = when(
        // N-to-1 should not propagate S
        (AM.src(LABEL) === LABEL_PS or (AM.src(LABEL) === LABEL_PT_PS and AM.src(OUT) > 1))
          and AM.dst(LABEL) === LABEL_B,
        LABEL_S)
        .when(AM.dst(LABEL) === LABEL_PT_PS, LABEL_PT)
        .otherwise(AM.dst(LABEL))
      // PT and PT/PS propagate to src
      val msgToSrc: Column = when(
        // 1-to-N should not propagate T
        (AM.dst(LABEL) === LABEL_PT or (AM.dst(LABEL) === LABEL_PT_PS and AM.dst(IN) > 1))
          and (AM.src(LABEL) === LABEL_S or AM.src(LABEL) === LABEL_B or AM.src(LABEL) === LABEL_PS),
        LABEL_T)
        .when(AM.src(LABEL) === LABEL_PT_PS, LABEL_PT)
        .when(AM.src(LABEL) === LABEL_PS, LABEL_T)
        .otherwise(AM.src(LABEL))
      def aggFunc(msg: Column): Column = collect_set(AM.msg)

      val vPTPS = graph.getPTPS
      val cachedGraph = GraphFrame(vPTPS.vertices.persist(StorageLevel.MEMORY_ONLY_SER), vPTPS.edges)
      cachedGraph.vertices.count()  // load computation into memory
      val propagatedVertices = cachedGraph
        .aggregateMessages
        .sendToSrc(msgToSrc)
        .sendToDst(msgToDst)
        .agg(aggFunc(AM.msg).as("label_list"))
        .withColumn(LABEL, pickLabelUDF(col("label_list")))
        .drop("label_list")
      val v = graph.vertices
        .join(propagatedVertices, Seq(ID), "left")
        .select(col(ID), coalesce(propagatedVertices(LABEL), graph.vertices(LABEL)).as(LABEL), col(IN), col(OUT))
        .withColumn(LABEL, modifyPTPSUDF(col(LABEL), col(IN), col(OUT)))
        .drop(IN, OUT)
      GraphFrame(v, graph.edges)
    }

    def getPTPS: GraphFrame = getSubGraph(col("src.label") === LABEL_PS or
      col("dst.label") === LABEL_PT or
      col("src.label") === LABEL_PT_PS or
      col("dst.label") === LABEL_PT_PS)

    private def getSubGraph(condition: Column): GraphFrame = {
      val e = graph.triplets.filter(condition)
      GraphFrame(graph.vertices, e.select("edge.src", "edge.dst", "edge.overlapLen"))
    }

    def splitPath(divisor: Int): GraphFrame = {
      val edges = graph.triplets
        .filter(col("src.label") === LABEL_B and
          col("dst.label") === LABEL_B and
          col("src.id") % divisor === 0 and
          col("dst.id") % divisor =!= 0)
      val splittedV = edges.select("src.id", "src.label") union edges.select("dst.id", "dst.label")
      val t = edges.select("src.id").withColumn(LABEL, lit(LABEL_T))
      val s = edges.select("dst.id").withColumn(LABEL, lit(LABEL_S))
      val updatedVertices = t union s
      val v = graph.vertices.except(splittedV).union(updatedVertices)

      GraphFrame(v, graph.edges)
    }

    // edge output: (label: String, src: Long, dst: Long, overlapLen: Int)
    def addDstLabelToEdges(): GraphFrame = {
      val edgeAndDst = graph.triplets.drop(SRC)
      val e = edgeAndDst.select("dst.label", AssemblyEdge.columnOrder.map(i => s"edge.$i"): _*)
        .toDF(LABEL +: AssemblyEdge.columnOrder: _*)

      GraphFrame(graph.vertices, e)
    }

    // output vertex: (id: Long, label: String, start: Long, overlapLen: Int, rank: Int)
    def traverse(numIter: Int): GraphFrame = {
      val traverseTmpPath = "/traverse-tmp/" + randomUUID().toString

      // input: (id: Long, label: String, start: Long, overlapLen: Int, rank: Int)
      @tailrec
      def traverseAux(current: DataFrame, previousCache: List[DataFrame], edges: DataFrame, result: DataFrame, i: Int): DataFrame = {
        if (i > numIter || current.count == 0) {
          println("iter: " + i)
          previousCache.map(_.unpersist())
          (result union spark.read.parquet(s"$traverseTmpPath/*")).repartition(col(START))
        }
        else {
          val oneStep = edges
            .join(current.drop(OVERLAP, RANK), col(ID) === col(SRC), "left")
            .persist(StorageLevel.MEMORY_ONLY_SER)
          oneStep.count() // TODO: verify performance

          val next = oneStep.na.drop(Seq(START))
            .select(col(DST).as(ID), current(LABEL), col(START), edges(OVERLAP), edges(LABEL).as("currentLabel"))
            .withColumn(RANK, lit(i))
          val nextVertices = next.filter(col("currentLabel") === LABEL_B)
          val unvisitedEdges = oneStep.filter(col(START).isNull).select(edges.columns.map(edges(_)): _*)

          next.drop(col("currentLabel")).write.parquet(s"$traverseTmpPath/$i")

          if (i % 7 == 0) {
            val n = nextVertices.checkpoint()
            val u = unvisitedEdges.checkpoint()
            previousCache.tail.map(_.unpersist())
            traverseAux(n, List.empty, u, result, i + 1)
          } else {
            traverseAux(nextVertices, oneStep :: previousCache, unvisitedEdges, result, i + 1)
          }
        }
      }

      val sVertices = graph.vertices
        .filter(col(LABEL) === LABEL_S)
        .withColumn(START, col(ID))
        .withColumn(OVERLAP, lit(0))
        .withColumn(RANK, lit(0))
      val edges = graph.edges.filter(graph.edges(LABEL) === LABEL_B or graph.edges(LABEL) === LABEL_T)
      val result = traverseAux(sVertices, List.empty, edges, sVertices, 1)
      val v = result.join(graph.vertices.withColumnRenamed(LABEL, "origLabel"), Seq(ID), "right")
        .withColumn(LABEL, coalesce(col(LABEL), col("origLabel")))
        .withColumn(START, coalesce(col(START), col(ID)))
        .na.fill(Map(OVERLAP -> 0, RANK -> 0))
        .drop("origLabel")

      GraphFrame(v, graph.edges)
    }

    // output vertex: (reads: Seq[Long], seq: String, qual: String, label: String, id: Long, endId: Long)
    def concatReads(firstAssembledVertices: DataFrame)(implicit spark: SparkSession): GraphFrame = {
      val v = if (firstAssembledVertices.columns.contains("reads")) {
        firstAssembledVertices.select(col(ID), col(SEQ).as("firstSeq"), col("reads").as("firstReads"), col(QUAL))
      } else {
        val genReadsUdf = udf((id: Long) => Seq(id))
        firstAssembledVertices.withColumn("reads", genReadsUdf(col(ID)))
          .select(col(ID), col(SEQ).as("firstSeq"), col("reads").as("firstReads"), col(QUAL))
      }

      assembleReads(v)
      { case ((seq: StringBuilder, qual: StringBuilder, label: Byte, _: Long), v: RankedVertex) =>
        val (q1, overlap1) = qual.splitAt(qual.length - v.overlapLen)
        val (overlap2, q2) = v.qual.splitAt(v.overlapLen)
        val decoded1 = decodeQual(overlap1.toString)
        val decoded2 = decodeQual(overlap2)
        val merged = mergeQualAndDepth(List(decoded1, decoded2))
        (seq ++= v.firstSeq.substring(v.overlapLen), q1 ++= merged ++= q2, label, v.firstReads.head)
      }
    }

    // input vertex: (id: Long, label: String, start: Long, overlapLen: Int, rank: Int)
    // seqVertices: (id: Long, firstSeq: String, firstReads: Seq[Long], qual: String)
    // output vertex: (reads: Seq[Long], seq: String, qual: String, label: String, id: Long, endId: Long)
    private def assembleReads(seqVertices: DataFrame)
                             (assembleFunc: ((StringBuilder, StringBuilder, Byte, Long), RankedVertex) => (StringBuilder, StringBuilder, Byte, Long))
                             (implicit spark: SparkSession): GraphFrame = {
      val startIdUDF = udf((reads: Seq[Long]) => { reads.head })

      import spark.implicits._
      val vertexEncoder = Encoders.product[RankedVertex]

      val contigs = graph.vertices
        .join(seqVertices, Seq(ID), "left")
        .as[RankedVertex](vertexEncoder)
        .groupByKey(_.start)
        .mapGroups{ case (_, vertices) =>
          val sortedVertices = vertices.toArray.sortBy(_.rank)
          val reads = sortedVertices.flatMap(_.firstReads)
          val seq = new StringBuilder(sortedVertices.head.firstSeq)
          val qual = new StringBuilder(sortedVertices.head.qual)
          val label = sortedVertices.head.label
          val endId = sortedVertices.head.firstReads.head
          val result = sortedVertices.tail.foldLeft((seq, qual, label, endId))(assembleFunc)
          (reads, result._1.toString, result._2.toString, result._3, result._4)
        }
        .toDF("assembled_reads", "assembled_seq", "assembled_qual", "assembled_label", "assembled_endId")
        .withColumn("assembled_startId", startIdUDF(col("assembled_reads")))
        .select(col("assembled_reads").as("reads"),
          col("assembled_seq").as(SEQ),
          col("assembled_qual").as(QUAL),
          col("assembled_label").as(LABEL),
          col("assembled_startId").as(ID),
          col("assembled_endId").as("endId"))

      GraphFrame(contigs, graph.edges)
    }

    // input vertex: (id: Long, label: String, start: Long, overlapLen: Int, rank: Int)
    // edge output: (src: Long, dst: Long, overlapLen: Int)
    def updateEdges()(implicit spark: SparkSession): GraphFrame = {
      val e = graph.triplets
        .filter(col("dst.start") === col("dst.id") or
          // 2 situations should be considered
          // case 1:
          // T ---+             +---> S
          // T ---+---> PT/PS --+---> S
          // T ---+             +---> S
          // PT/PS will be PT after propagation
          // case 2:
          // src and dst are PS and PT, after label propagation they are both PT
          col("src.label") === LABEL_PT or
          col("dst.label") === LABEL_PT)
        .select(col("src.start").as(SRC),
          col("dst.start").as(DST),
          col("edge.overlapLen"))

      GraphFrame(graph.vertices, e)
    }

    def cacheGraph(cacheVertices: Boolean = true, cacheEdges: Boolean = true): GraphFrame = {
      val v = if (cacheVertices) {
        val checkpointV = graph.vertices.persist(StorageLevel.MEMORY_ONLY_SER).checkpoint()
        graph.vertices.unpersist()
        checkpointV
      } else {
        graph.vertices
      }
      val e = if (cacheEdges) {
        val checkpointE = graph.edges.persist(StorageLevel.MEMORY_ONLY_SER).checkpoint()
        graph.edges.unpersist()
        checkpointE
      } else {
        graph.edges
      }

      GraphFrame(v, e)
    }

    def denoise(contigLen: Int): GraphFrame = {
      import spark.implicits._

      val e = graph.triplets
        .filter(length($"src.seq") > contigLen or length($"dst.seq") > contigLen)
        .filter("src.id != dst.id")
        .select("edge.src", "edge.dst", "edge.overlapLen")

      GraphFrame(graph.vertices, e)
    }

    def relabel(): GraphFrame = {
      // N-to-1 and 1-to-N vertices are labeled as S and T respectively,
      // thus we need to relabeled as PT/PS for pairReads()
      val label = graph.degree().label().vertices
      val v = graph.vertices.drop(LABEL).join(label, Seq(GraphFrame.ID), "left")

      GraphFrame(v, graph.edges)
    }

    def removeDanglingEdges(): GraphFrame = {
      val e = graph.edges
        .join(graph.vertices, col(SRC) === col(ID), "leftsemi")
        .join(graph.vertices, col(DST) === col(ID), "leftsemi")

      GraphFrame(graph.vertices, e)
    }

    def truncateReads(expectedElements: Int): GraphFrame = {
      import spark.implicits._
      val encoder = Encoders.product[OneToOneVertex]
      val v = graph.vertices
        .as[OneToOneVertex](encoder)
        .mapPartitions{ iter =>
          iter.map { v =>
            val reads = v.reads.take(expectedElements) ++ v.reads.takeRight(expectedElements)
            v.copy(reads = reads)
          }
        }

      GraphFrame(v.toDF(), graph.edges)
    }

    // connectReads() will start from S and stop at T
    // Thus we have to propagate PS and PT info to T and S respectively
    def pairPreprocess(intersection: Long, diploid: Int): (GraphFrame, DataFrame) = {
      import spark.implicits._
      val vertexEncoder = Encoders.product[OneToOneVertex]
      val edgeEncoder = Encoders.product[OneToOneEdge]
      val encoder = Encoders.product[OneToOneTriplet]

      val psTriplets = graph.triplets
        .filter(col("src.label") === LABEL_PS and col("dst.label") === LABEL_T and col("dst.inDegree") === 1)
      val update1 = psTriplets.as[OneToOneTriplet](encoder)
        .groupByKey(_.src)
        .flatMapGroups { case (_, triplets) =>
          val (qualified, unqualified) = triplets.map { t => t.src.reads.intersect(t.dst.reads).length -> t }
            .partition(_._1 >= intersection)

          val (t1, t2) = qualified.toArray
            .sortBy(_._1)(Ordering.Int.reverse)
            .splitAt(diploid)

          t1.map { p =>
            val t = p._2
            val (q1, overlap1) = t.src.qual.splitAt(t.src.qual.length - t.edge.overlapLen)
            val (overlap2, q2) = t.dst.qual.splitAt(t.edge.overlapLen)
            val decoded1 = decodeQual(overlap1.toString)
            val decoded2 = decodeQual(overlap2)
            val merged = mergeQualAndDepth(List(decoded1, decoded2))
            t.dst.copy(id = t.src.id,
              reads = t.src.reads ++ t.dst.reads,
              seq = t.src.seq + t.dst.seq.substring(t.edge.overlapLen),
              qual = q1 ++ merged ++ q2)
          } ++ (t2 ++ unqualified).map(_._2.dst)
        }

      val ptTriplets = graph.triplets
        .filter(col("src.label") === LABEL_S and col("dst.label") === LABEL_PT)
      val update2 = ptTriplets.as[OneToOneTriplet](encoder)
        .groupByKey(_.dst)
        .flatMapGroups { case (_, triplets) =>
          val (qualified, unqualified) = triplets.map { t => t.src.reads.intersect(t.dst.reads).length -> t }
            .partition(_._1 >= intersection)

          val (t1, t2) = qualified.toArray
            .sortBy(_._1)(Ordering.Int.reverse)
            .splitAt(diploid)

          t1.map { p =>
            val t = p._2
            val (q1, overlap1) = t.src.qual.splitAt(t.src.qual.length - t.edge.overlapLen)
            val (overlap2, q2) = t.dst.qual.splitAt(t.edge.overlapLen)
            val decoded1 = decodeQual(overlap1.toString)
            val decoded2 = decodeQual(overlap2)
            val merged = mergeQualAndDepth(List(decoded1, decoded2))
            t.src.copy(endId = t.dst.endId,
              reads = t.src.reads ++ t.dst.reads,
              seq = t.src.seq + t.dst.seq.substring(t.edge.overlapLen),
              qual = q1 ++ merged ++ q2)
          } ++ (t2 ++ unqualified).map(_._2.src)
        }

      val updatedVerticesId = update1.map(_.id)
        .union(update2.map(_.id))
        .distinct()
        .toDF("updated_id")
      val unchangedV = graph.vertices.join(updatedVerticesId, col(ID) === col("updated_id"), "left_anti")
      val stPairV = (update1 union update2).select( SEQ, QUAL, ID, "endId").toDF()

      GraphFrame(unchangedV, graph.edges) -> stPairV.cacheDataFrame()
    }

    def slimVT(): (GraphFrame, DataFrame) = {
      val vtCache = graph.vertices.select(ID, SEQ, QUAL).persist(StorageLevel.MEMORY_ONLY_SER)
      val vt = vtCache.checkpoint()
      vtCache.unpersist()

      import spark.implicits._
      val encoder = Encoders.product[OneToOneVertex]
      val slimV = graph.vertices
        .as[OneToOneVertex](encoder)
        .mapPartitions { iter =>
          iter.map { v =>
            SlimOneToOneVertex(v.reads, v.seq.length, v.label, v.id, v.endId)
          }
        }
        .toDF()

      // TODO: should we checkpoint offloadV?
      GraphFrame(slimV, graph.edges) -> vt
    }

    // input vertex: (reads: Seq[Long], seq: String, label: String, id: Long, endId: Long)
    def pairReads(expectedElements: Int, falsePositiveRate: Double, intersection: Long, overlapLen: Int, ploidy: Int, numPartitions: Int)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val readTripletEncoder = Encoders.product[ReadTriplet]

      val t =  graph.triplets
        .filter(col("src.label") === LABEL_B or col("dst.label") === LABEL_B or
          col("src.label") === LABEL_PT_PS or col("dst.label") === LABEL_PT_PS)
        .repartition(numPartitions, col("src.id"), col("dst.id"))
      t.persist(StorageLevel.MEMORY_AND_DISK_SER).count()
      val pairs = markTriggerPoint(genPairs(genReadTriplet(t, LABEL_PT_PS), expectedElements, falsePositiveRate, intersection, overlapLen, ploidy, multi = true))
      pairs.persist(StorageLevel.MEMORY_AND_DISK_SER).count()

      val bTriplets = genReadTriplet(t, LABEL_B)  // add .persist(StorageLevel.MEMORY_AND_DISK_SER).count() may improve performance
      bTriplets.persist(StorageLevel.MEMORY_AND_DISK_SER).count
      val endpointBTriplets = bTriplets.filter(p => p.label1 != LABEL_PT_PS || p.label2 != LABEL_PT_PS)
      val endpointPairs = genPairs(endpointBTriplets, expectedElements, falsePositiveRate, intersection, overlapLen, ploidy)

      val outId = pairs.filter(p => p.out._1 != -1 && p.out._2 != -1)
        .map(_.out._2)
      val inId = pairs.filter(p => p.in._1 != -1 && p.in._2 != -1)
        .map(_.in._1)
      val nonBrokenPairIds = (outId intersect inId).toDF(ID)
      val bPairs = bTriplets.join(nonBrokenPairIds, bTriplets("jointId") === nonBrokenPairIds(ID), "leftsemi")
        .as[ReadTriplet](readTripletEncoder)
        .flatMap(Pair.toPairs(10000, _, false)) // TODO: score

      (pairs union endpointPairs union bPairs).toDF()
    }

    def record(vt: String, ed: String): GraphFrame = {
      graph.vertices.write.parquet(vt)
      graph.edges.write.parquet(ed)

      graph
    }

    def smallAssemble(mod: Int, smallSteps: Int, profiling: Boolean = false)(implicit spark: SparkSession): (GraphFrame, DataFrame) = {
      val g = graph.degree()
        .profilingDegree(profiling)
        .label()
        .cacheGraph(cacheEdges = false)
        .propagation()
        .splitPath(mod)
        .cacheGraph(cacheEdges = false)
        .addDstLabelToEdges()
        .traverse(smallSteps)
        .updateEdges()
        .concatReads(graph.vertices)
        .cacheGraph()

      g.dropIsolatedVertices() -> g.getIsolatedVertices()
    }

    def bigAssemble(bigSteps: Int, denoiseLen: Option[Int] = None)(implicit spark: SparkSession): (GraphFrame, DataFrame) = {
      val assembledGraph = graph.degree()
        .label()
        .cacheGraph(cacheEdges = false)
        .propagation()
        .addDstLabelToEdges()
        .cacheGraph()
        .traverse(bigSteps)
        .updateEdges()
        .concatReads(graph.vertices)

      val g = denoiseLen match {
        case Some(len) => assembledGraph.denoise(len).cacheGraph()
        case None => assembledGraph.cacheGraph()
      }

      g.dropIsolatedVertices() -> g.getIsolatedVertices()
    }
  }

  def genReadTriplet(triplets: DataFrame, label: Byte): Dataset[ReadTriplet] = {
    val readTripletEncoder = Encoders.product[ReadTriplet]

    val t1 = triplets
      .filter(col("dst.label") === label)
      .as("t1")
    val t2 = triplets
      .filter(col("src.label") === label)
      .as("t2")

    t1.join(t2, col("t1.dst.id") === col("t2.src.id") and
      col("t1.src.id") =!= col("t2.dst.id"))
      .select(col("t1.src.reads").as("reads1"), col("t1.src.id").as("id1"),
        col("t1.src.len").as("len1"),
        col("t1.src.label").as("label1"),
        col("t1.edge.overlapLen").as("overlapLen1"),
        col("t1.dst.reads").as("jointReads"), col("t1.dst.id").as("jointId"),
        col("t1.dst.len").as("jointLen"),
        col("t2.edge.overlapLen").as("overlapLen2"),
        col("t2.dst.reads").as("reads2"), col("t2.dst.id").as("id2"),
        col("t2.dst.len").as("len2"),
        col("t2.dst.label").as("label2"))
      .as[ReadTriplet](readTripletEncoder)
  }

  def genPairs(triplets: Dataset[ReadTriplet],
               expectedElements: Int,
               falsePositiveRate: Double,
               intersection: Long,
               overlapLen: Int,
               ploidy: Int,
               multi: Boolean = false)
              (implicit spark: SparkSession): Dataset[Pair] = {
    import spark.implicits._

    // in-degree major, select highest score triplet for each in-degree edge
    triplets
      .groupByKey(i => i.jointId -> i.id1)
      .flatMapGroups{ case (_, t) =>
        selectHighestScoreTriplet(t, expectedElements, falsePositiveRate, intersection, overlapLen)
      }
      .groupByKey(_._2.jointId)
      .flatMapGroups{ case (_, scoreAndTriplets) =>
        genPairsByPloidy(scoreAndTriplets, ploidy, multi)
      }
      .dropDuplicates("in", "out")
  }

  def selectHighestScoreTriplet(triplets: Iterator[ReadTriplet],
                                expectedElements: Int,
                                falsePositiveRate: Double,
                                intersection: Long,
                                overlapLen: Int): Iterable[((Option[Byte], Long), ReadTriplet)] = {
    def encode(id: Long): Long = {
      id & BLOOM_FILTER_ENCODE_MASK
    }

    def score[T](a: BloomFilter[T], b: BloomFilter[T]): Long = {
      val intersection = a.intersect(b)
      val count = intersection.approximateElementCount()

      intersection.dispose()
      count
    }

    def validate(score1: Long, scoreJ: Long, score2: Long, intersection: Long, overlapLen1: Int, overlapLen2: Int, overlapLen: Int): (Option[Byte], Long) = {
      def boolean2Int(b: Boolean): Int = if (b) 1 else 0
      val s1 = boolean2Int(score1 >= intersection)
      val sj = boolean2Int(scoreJ >= intersection)
      val s2 = boolean2Int(score2 >= intersection)

      if (s1 + sj + s2 >= 2 && overlapLen1 >= overlapLen && overlapLen2 >= overlapLen) {
        None -> (score1 + scoreJ + score2)
      } else if (sj > 0) {
        None -> scoreJ
      } else if (s1 > 0 && s1 > s2) {
        Some(BROKEN_TAIL) -> score1
      } else if (s2 > 0) {
        Some(BROKEN_HEAD) -> score2
      } else {
        Some(BROKEN_BOTH) -> 0
      }
    }

    val pairs = triplets.map { t =>
      val read1tail = t.reads1.takeRight(expectedElements)
      val read2head = t.reads2.take(expectedElements)

      val encoded1 = read1tail.map(encode)
      val encoded2 = read2head.map(encode)
      val encodedj = t.jointReads.map(encode)

      val s = (encoded1 intersect encoded2).size.toLong
      val scoreRead1tailJ = (encoded1 intersect encodedj).size.toLong
      val scoreJRead2head = (encodedj intersect encoded2).size.toLong

      val (brokenType, score) = validate(scoreRead1tailJ, s, scoreJRead2head, intersection, t.overlapLen1, t.overlapLen2, overlapLen)

      (brokenType, score) -> t.copy(jointReads = Seq(t.jointReads.last))
    }
      .toTraversable

    // for each in-degree edge select an highest score output edge
    val highestScore = pairs.maxBy(i => i._1._1.isEmpty -> i._1._2)(Ordering.Tuple2(Ordering.Boolean, Ordering.Long))

    List(highestScore)
  }

  def genPairsByPloidy(triplets: Iterator[((Option[Byte], Long), ReadTriplet)], ploidy: Int, multi: Boolean): Iterable[Pair] = {
    // tha max # of paired pairs for each PT/PS is args.ploidy
    // sorted by intersection # and overlap len then choose the first `ploidy` pairs
    // other pairs should be broken
    val pairsGroup = triplets.toArray
      .sortBy{ case ((_, score), t) => score -> t.overlapLen1 }(Ordering.Tuple2(Ordering.Long.reverse, Ordering.Int.reverse))
      .splitAt(ploidy)
    pairsGroup._1
      .flatMap{ case ((brokenType, score), t) =>
        brokenType.map(Pair.toBrokenPairs(_, score, t, multi))
          .getOrElse(Pair.toPairs(score, t, multi))
      } ++
      pairsGroup._2
        .flatMap{ case ((brokenType, score), t) =>
          brokenType.map(Pair.toBrokenPairs(_, score, t, multi))
            .getOrElse(Pair.toBrokenPairs(BROKEN_TAIL ,score, t, multi))
        }
  }

  def markTriggerPoint(ds: Dataset[Pair])(implicit spark: SparkSession): Dataset[Pair] = {
    import spark.implicits._

    ds.groupByKey(_.out)
      .flatMapGroups { case (out, iter) =>
        val pairs = iter.toList
        // this kind of pairs will be the end of contig, no necessary to be trigger point
        // a -+
        //    |--> b -> -1
        // c -+
        if (pairs.length == 1 || out._2 == -1) {
          pairs
        } else {
          pairs.map(_.copy(trigger = true))
        }
      }
  }
}
