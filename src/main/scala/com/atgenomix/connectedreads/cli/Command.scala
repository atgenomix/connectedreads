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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli.Args4jBase

trait CommandCompanion {
  val commandName: String
  val commandDescription: String

  def apply(cmdLine: Array[String]): Command
}

trait Command extends Runnable {
  val companion: CommandCompanion
}

trait SparkCommand[A <: Args4jBase] extends Command {
  protected val args: A

  def run(spark: SparkSession)

  def run(): Unit = {
    val conf = new SparkConf()
      // [Using Apache Spark with Amazon S3] (https://hortonworks.github.io/hdp-aws/s3-spark/)
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.parquet.mergeSchema", "false")
      .set("spark.hadoop.parquet.enable.summary-metadata", "false")
      .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("mapreduce.fileoutputcommitter.cleanup.skipped", "true")
      .setAppName("GraphSeq: " + companion.commandName)
    if (conf.getOption("spark.master").isEmpty) {
      conf.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val e = try {
      run(spark)
      None
    } catch {
      case e: Throwable =>
        System.err.println("Command body threw exception:\n%s".format(e))
        Some(e)
    }
    e.foreach(throw _)
  }
}