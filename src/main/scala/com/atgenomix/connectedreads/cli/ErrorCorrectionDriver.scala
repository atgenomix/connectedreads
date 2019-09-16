/**
  * Copyright (C) 2015, Atgenomix Incorporated. All Rights Reserved.
  * This program is an unpublished copyrighted work which is proprietary to
  * Atgenomix Incorporated and contains confidential information that is not to
  * be reproduced or disclosed to any other person or entity without prior
  * written consent from Atgenomix, Inc. in each and every instance.
  * Unauthorized reproduction of this program as well as unauthorized
  * preparation of derivative works based upon the program or distribution of
  * copies by sale, rental, lease or lending are violations of federal copyright
  * laws and state trade secret laws, punishable by civil and criminal penalties.
  */

package com.atgenomix.connectedreads.cli

import java.io._
import java.nio.file.Paths

import com.atgenomix.connectedreads.pipeline.ErrorCorrectionPipeline
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli.{Args4j, Args4jBase}
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object ErrorCorrectionDriver extends CommandCompanion with Serializable {
  override val commandName: String = "correct"
  override val commandDescription: String = "conduct error correction on input reads"

  override def apply(cmdLine: Array[String]): Command = new ErrorCorrectionDriver(Args4j[ErrorCorrectionArgs](cmdLine))
}

class ErrorCorrectionArgs extends Args4jBase with Serializable with GraphSeqArgsBase
{
  @Argument(required = true, metaVar = "INPUT", usage = "Input path (generated by Adam transform)", index = 0)
  override var input: String = _

  @Argument(required = true, metaVar = "OUTPUT", usage = "Output path", index = 1)
  override var output: String = _

  @Args4jOption(required = true, name = "-pl_batch", usage = "Prefix length for number of batches [default=1]")
  override var pl: Short = 1

  @Args4jOption(required = true, name = "-pl_partition", usage = "Prefix length for number of partitions [default=7]")
  override var pl2: Short = 7

  @Args4jOption(required = false, name = "-packing_size", usage = "The number of reads will be packed together [default = 100]")
  override var packing_size: Int = 100

  // TODO: renaming as min_correction_pos, indicating the minimum depth for internal nodes bottom-up scanning
  @Args4jOption(required = false, name = "-mlcp", usage = "Minimal longest common prefix [default = 45]")
  override var minlcp: Int = 45

  @Args4jOption(required = false, name = "-max_read_length", usage = "Maximal read length [default = 151]")
  override var maxrlen: Int = 151

  @Args4jOption(required = false, name = "-stats", usage = "Enable to output statistics of String Graph to $OUTPUT/STATS")
  override var stats: Boolean = false

  @Args4jOption(required = false, name = "-profiling", usage = "Enable performance profiling and output to $OUTPUT/STATS")
  override var profiling: Boolean = false

  @Args4jOption(required = false, name = "-max_N_count", usage = "upper limit for uncalled base count")
  override var maxNCount: Int = 10

  @Args4jOption(required = false, name = "-assign_N", usage = "whether to randeomly replace N base in reads with A/C/G/T base")
  override var assignN: Boolean = false

  @Args4jOption(required = false, name = "-max_err_read", usage = "maximal reads having errors at a certain internal node")
  var maxerrread: Int = 2

  @Args4jOption(required = false, name = "-min_read_support", usage = "minimal reads support for error identification at a certain internal node")
  var minreadsupport: Int = 20

  @Args4jOption(required = false, name = "-max_correction_ratio", usage = "maximal error over correction target base ratio for error identification at a certain internal node")
  var maxcorratio: Double = 0.05

  @Args4jOption(required = false, name = "-total_ploidy", usage = "total ploidy of input fastq file and reference files")
  var totalploidy: Int = 2

  @Args4jOption(required = false, name = "-ref_samples_path", usage = "provide reference sample path with space as deliminator, e.g. path1 path2 ... pathN", handler = classOf[StringArrayOptionHandler])
  var ref_samples: Array[String] = Array()

  @Args4jOption(required = false, name = "-output_fastq", usage = "whether to dump all reads to fastq file")
  var outputFastq: Boolean = false

  @Args4jOption(required = false, name = "-mim_err_depth", usage = "minimal depth of suffix tree where error will be reported")
  var minSufTreeDepth: Int = 40

  @Args4jOption(required = false, name = "-raw_err_group_len", usage = "range where raw error reports should be grouped as a single error event")
  var rawErrGroupLen: Int = 10

  @Args4jOption(required = false, name = "-raw_err_cutoff", usage = "minimum threshold of raw error reports")
  var rawErrCutoff: Int = 5

  @Args4jOption(required = false, name = "-keep_err", usage = "whether to keep error report in tsv format")
  var keepErr: Boolean = false

  @Args4jOption(required = false, name = "-seperate_err", usage = "seperate error")
  var seperateErr: Boolean = false
}

case class ErrReport(readID: Long, rc: Boolean, errPos: Int, depth: Int, err: String, cor: String)

class ErrorCorrectionDriver(val args: ErrorCorrectionArgs) extends Serializable with SparkCommand[ErrorCorrectionArgs] {

  // output folder path
  val _rawErrFolder = "RAWERR"
  val _rlErrFolder = "ERR"
  val _targetFqFolder = "TARGET.FQ.ADAM"
  val _corFqFolder = "COR.FQ.ADAM"
  val _statisticFolder = "STATS"
  val _fqFolder = "IN.FQ.TEXT"
  val _statsPath: String = Paths.get(args.output, _statisticFolder).toString

  // pos data structure coding constant
  val nameBitLen = 40
  val _nameMask = 0x000000FFFFFFFFFFL
  val _offsetMask = 0x00FFFF0000000000L
  val _rcMask = 0x4000000000000000L
  val _refReadMask = 0x2000000000000000L


  override val companion: CommandCompanion = ErrorCorrectionDriver

  override def run(spark: SparkSession): Unit = {
    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)

    val ect = new ErrorCorrectionPipeline(spark, args, nameBitLen, _nameMask, _offsetMask, _rcMask, _refReadMask)

    // read from adam parquet
    val numberedReads = ect.loadReads(args.input, args.ref_samples)

    // write input reads
    if (args.outputFastq)
      ect.writeFastq(numberedReads, Paths.get(args.output, _fqFolder).toString)

    // find raw level error
    ect.reportRawErrCand(numberedReads, args.pl, _statsPath, args.output, _rawErrFolder)

    // find read level error
    val err =
      ect.findErr(Paths.get(args.output, _rawErrFolder, "*/*").toString, args.rawErrGroupLen, args.rawErrCutoff).cache()

    // write error report to file
    if (args.keepErr) ect.writeErr(err, Paths.get(args.output, _rlErrFolder).toString)
    else fs.delete(new Path(Paths.get(args.output, _rawErrFolder, "*/*").toString), true)

    // correct input fastq with error report
    ect.correct(err, args.seperateErr, args.input, Paths.get(args.output, _corFqFolder).toString)

    // stats information aggregation
    if (args.profiling)
      spark.sparkContext.textFile(Paths.get(_statsPath, "profiling").toString)
        .coalesce(1)
        .saveAsTextFile(Paths.get(_statsPath, "profiling-summary").toString)
    fs.delete(new Path(Paths.get(_statsPath, "profiling").toString), true)
  }
}
