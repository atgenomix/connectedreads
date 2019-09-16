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

package com.atgenomix.connectedreads.core.rdd

import org.apache.avro.specific.SpecificRecord
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.bdgenomics.utils.misc.HadoopUtil

import scala.reflect.ClassTag

object SparkSTContext {
  implicit def sparkSessionToSeqMLContext(spark: SparkSession): SparkSTContext = new SparkSTContext(spark)
}

case class SeqPos(seq: String, pos: Long)

class SparkSTContext(@transient val spark: SparkSession) extends Serializable {
  // FASTA
  def loadParquetContigFragments(fp: String*): Fragments = {
    val columnNames = Seq("contig.contigName", "fragmentSequence", "fragmentStartPosition")
    Fragments(spark, spark.read.option("mergeSchema", "false").parquet(fp: _*)
      .select(columnNames.head, columnNames.tail: _*))
  }

  // FASTQ
  def loadParquetAlignments(fp: String*): Fragments = {
    val columnNames = Seq("readName", "sequence")
    Fragments(spark, spark.read.option("mergeSchema", "false").parquet(fp: _*)
      .select(columnNames.head, columnNames.tail: _*))
  }

  def loadParquetAsDataset[T: Encoder](path: String): Dataset[T] = spark.read.parquet(path).as[T]

  def loadParquetAsDataFrame(path: String): DataFrame = spark.read.parquet(path)

  def loadParquet[T <: SpecificRecord](path: String)(implicit ct: ClassTag[T]): RDD[T] = {
    val job = HadoopUtil.newJob(spark.sparkContext)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    val records = spark.sparkContext.newAPIHadoopFile(
      path,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      ct.runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job))
    records.map(_._2)
  }
}
