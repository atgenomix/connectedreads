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

import com.atgenomix.connectedreads.core.model.TwoBitSufSeq
import com.atgenomix.connectedreads.core.rdd.SparkSTContext._
import com.atgenomix.connectedreads.core.rdd.{CommonPrefix, KeyPartitioner4, Suffixes}
import com.atgenomix.connectedreads.core.util.SuffixUtils.suffix
import com.atgenomix.connectedreads.io.FileFormat
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli.{Args4j, Args4jBase}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object Fasta2GDF extends CommandCompanion {
  override val commandName: String = "fasta2gdf"
  override val commandDescription: String = "Convert SAM/BAM/hg19 to DataFrames that represent edges of suffix indices"

  override def apply(cmdLine: Array[String]): Command = new Fasta2GDF(Args4j[Fasta2GDFArgs](cmdLine))
}

class Fasta2GDFArgs extends Args4jBase with AWSArgs with Serializable {
  @Argument(required = true, metaVar = "FASTA", usage = "The FASTA file to convert", index = 0)
  var fastaPath: String = _

  @Argument(required = true, metaVar = "GDF", usage = "Location to write the transformed data in GDF/Parquet format", index = 1)
  var outputPath: String = _

  @Args4jOption(required = true, name = "-prefix_length", usage = "Prefix length for number of partition")
  var pl: Short = _

  @Args4jOption(required = false, name = "-min_suffix_length", usage = "Minimal suffix length")
  var minsfx: Int = _ // $ is excluded

  @Args4jOption(required = false, name = "-query_length", usage = "The maximal length of the query sequence (default = 101)")
  var l: Int = 101
}

class Fasta2GDF(val args: Fasta2GDFArgs) extends Serializable with SparkCommand[Fasta2GDFArgs] {
  require(args.pl <= args.minsfx, "prefix length should less than or equal minimal suffix length")
  val companion: CommandCompanion = Fasta2GDF

  def checkfs[A <: AWSArgs](spark: SparkSession, args: A): Unit = {
    if (args.awsAccessKeyId != null && args.awsSecretAccessKey != null) {
      spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      spark.conf.set("fs.s3a.access.key", args.awsAccessKeyId)
      spark.conf.set("fs.s3a.secret.key", args.awsSecretAccessKey)
    }
  }

  override def run(spark: SparkSession) {
    checkfs(spark, args)
    val fragments = spark.loadParquetContigFragments(args.fastaPath)
    val segments = fragments.toPairedSegments(FileFormat.FASTA, args.minsfx, args.l) // (FF, Sequence, Position)
    import spark.implicits._
    val suffixes = segments
      .flatMap(x => suffix(x._1, x._2, args.pl, args.minsfx))
      .rdd.partitionBy(new KeyPartitioner4(args.pl)).toDS()
      .flatMap(x => x._2._2.map(offset => TwoBitSufSeq(seq = x._1, offset = offset, length = (x._2._3 - offset).toShort, idx = 0, pos = x._2._1 + offset)))
      .mapPartitions(_.toList.sorted.iterator) // 1st sort

    val cp = suffixes.mapPartitions(iter => CommonPrefix.run(iter, args.pl))
    val gs = cp.mapPartitions(_.toList.sorted.iterator).mapPartitions(Suffixes.index(_, args.pl))

    gs.mapPartitions(Suffixes.edges(_, args.pl))
      .toDF("src", "dst", "src_len", "dst_len", "next_bases", "pos_list")
      .write.option("compression", "snappy").parquet(args.outputPath)
  }
}