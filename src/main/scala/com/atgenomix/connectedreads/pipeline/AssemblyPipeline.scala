package com.atgenomix.connectedreads.pipeline

import com.atgenomix.connectedreads.cli.AssemblyArgs
import com.atgenomix.connectedreads.core.util.AssembleUtils._
import com.atgenomix.connectedreads.core.util.GraphUtils._
import com.atgenomix.connectedreads.core.util.LabelUtils._
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

class AssemblyPipeline(@transient val spark: SparkSession, args: AssemblyArgs) extends Serializable {
  def assemble() = {
    implicit val session = spark

    val inputGraph = getGraph()
    val isolatedVertices = inputGraph.getIsolatedVertices()
    val (smallGraph, smallIsolatedV) = inputGraph.dropIsolatedVertices()
      .expandGraph()
      .smallAssemble(args.mod, args.smallSteps, args.degreeProfiling)
    val (bigGraph, bigIsolatedV) = smallGraph.bigAssemble(args.bigSteps, Some(args.denoiseLen))

    // denoise might generate N-to-1/1-to-N vertices. Thus need to assemble again
    val (smallGraph2, smallIsolatedV2) = bigGraph.smallAssemble(args.mod, args.smallSteps)
    val (bigGraph2, bigIsolatedV2) = smallGraph2.bigAssemble(args.bigSteps)
    val one2oneGraph = bigGraph2.relabel()
    val isolatedV = isolatedVertices union smallIsolatedV union bigIsolatedV union smallIsolatedV2 union bigIsolatedV2

    if (args.oneToOneProfiling) {
      one2oneGraph.record(args.output + "/one2one/vt", args.output + "/one2one/ed")
      isolatedV.write.parquet(args.output + "/one2one/isolated-vt")
      one2oneGraph.vertices.adamOutput[OneToOneVertex](args.output + "/one2one/vt-adam")
    }

    val (graphToPair, stPairVertices) = one2oneGraph
      .truncateReads(args.expectedElements)
      .pairPreprocess(args.intersection, args.ploidy)
    val (slimGraph, one2oneSeq) = graphToPair.slimVT()

    slimGraph
      .pairReads(args.expectedElements, args.falsePositiveRate, args.intersection, args.overlapLen, args.ploidy, args.partition)
      .cacheDataFrame()
      .rmRedundantBrokenPair()
      .updatePairLabel()
      .removeRedundantPairs()
      .cacheDataFrame()
      .connectReads(args.contigUpperBound)
      .rmRedundantSingleVertex()
      .genContigs(one2oneSeq)
      .union(isolatedV union stPairVertices)
      .removeDuplicated()
      .adamOutput[Contig](args.output + "/result")
  }

  private def getGraph(): GraphFrame = {
    val columns = List("src", "dst", "RS_start", "RS_end", "RS_length", "RP_start", "RP_end", "RP_length", "is_rc")
    if (args.inputFormat == "PARQUET") {
      val v = spark.read
        .parquet(args.vertexInput)
        .toDF("id", "seq", "qual")
      val e = spark.read
        .parquet(args.edgeInput)
        .withColumnRenamed("RS_ID", "src")
        .withColumnRenamed("RP_ID", "dst")
        .select(columns.head, columns.tail: _*) // column reorder for normalization

      GraphFrame(v, e)
    } else {
      // ASQG format
      val v = splitColumn(
        spark.read.text(args.vertexInput),
        "value",
        List("VT", "id", "seq", "qual", "tag")
      ).drop("VT")
        .drop("tag")

      val e = splitColumn(
        spark.read.text(args.edgeInput),
        "value",
        "ED" :: columns
      ).drop("ED")

      GraphFrame(v, e)
    }
  }
}

