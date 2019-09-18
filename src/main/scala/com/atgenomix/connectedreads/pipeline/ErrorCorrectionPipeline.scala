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

package com.atgenomix.connectedreads.pipeline

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.file.Paths

import com.atgenomix.connectedreads.cli.ErrorCorrectionArgs
import com.atgenomix.connectedreads.core.algorithm.{SuffixArray, SuffixTree}
import com.atgenomix.connectedreads.core.model._
import com.atgenomix.connectedreads.core.rdd.KeyPartitioner3
import com.atgenomix.connectedreads.core.util.AtgxReadsInfoParser
import com.atgenomix.connectedreads.core.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.asc
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.sql.{AlignmentRecord => AlignmentRecordProduct}
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.switch
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class ErrorEntry(readID: Long, rc: Boolean, errPos: Int, depth: Int, err: String, cor: String)


class ErrorCorrectionPipeline(@transient val spark: SparkSession, args: ErrorCorrectionArgs,
                              nameBitLen: Int = 40,
                              nameMask: Long = 0x000000FFFFFFFFFFL,
                              offsetMask: Long = 0x00FFFF0000000000L,
                              rcMask: Long = 0x4000000000000000L,
                              refReadMask: Long = 0x2000000000000000L) extends Serializable {

  type SuffixArray = (Int, Array[Long], Array[Byte], Array[Short])
  type ErrorEntry = com.atgenomix.connectedreads.pipeline.ErrorEntry

  import spark.implicits._

  def loadReads(inputPath: String, ref_samples: Array[String]): Dataset[NumberedReads] = {
    // read from adam parquet
    val inputReads = loadNumberTaggedReads(spark, inputPath, Seq("readName", "sequence", "qual"))
    if (ref_samples.length > 0) {
      val refReads = ref_samples
        .map(x => loadNumberTaggedReads(spark, x, Seq("readName", "sequence", "qual"), ref_flag = true))
        .reduce(_.union(_))
      inputReads.union(refReads)
    }
    else
      inputReads
  }

  def writeFastq(numberedReads: Dataset[NumberedReads], path: String): Unit = {
    numberedReads.rdd
      .map(x => {
        "@%012d\n%s\n+\n".format(x.sn, x.sequence) + "F" * x.sequence.length
      })
      .saveAsTextFile(path, classOf[GzipCodec])
  }

  def reportRawErrCand(numberedReads: Dataset[NumberedReads],
                       pl: Int, statsPath: String, outPath: String, rawErrFolder: String): Unit = {
    val batches: Int = scala.math.pow(4, pl).toInt
    for (i <- 0 until batches) {
      buildSA(numberedReads, statsPath, i)
        .mapPartitions(generateSS(_, statsPath, i))
        .mapPartitions(generateST(_, statsPath, i))
        .write.parquet(Paths.get(outPath, rawErrFolder, i.toString).toString)
    }
  }

  def findErr(rawPath: String, rawErrGroupLen: Int, rawErrCutoff: Int): RDD[ErrorEntry] = {
    // readLevel error report implementation based on rawErrReport
    val errorEncoder = Encoders.product[ErrorEntry]
    spark.read
      .option("mergeSchema", "false")
      .parquet(rawPath)
      .as[ErrorEntry](errorEncoder)
      .rdd
      .keyBy(x => x.readID)
      .groupByKey
      .map(readLevelCollapse)
      .map(readLevelGroup(_, rawErrGroupLen))
      .flatMap(readLevelReport(_, rawErrCutoff))
  }

  def writeErr(err: RDD[ErrorEntry], path: String): Unit = {
    err
      .toDF()
      .orderBy(asc("readID"))
      .select("readID", "errPos", "err", "cor")
      .write
      .mode("overwrite")
      .parquet(path)
  }

  def correct(err: RDD[ErrorEntry], seperateErr: Boolean, inPath: String, outPath: String): Unit = {
    // correct input fastq with error report
    val rdd = spark.read.option("mergeSchema", "false").parquet(inPath).as[AlignmentRecordProduct]
      .map(x => {
        val (_, iw) = AtgxReadsInfoParser.parseFromName(x.readName.get)
        (iw.getID, x)})
      .rdd
      .leftOuterJoin(err.keyBy(x => x.readID).groupByKey)

    val correctedRdd = rdd.filter(_._2._2.nonEmpty)
      .map(
        x => {
          var corSeq = x._2._1.sequence.get
          x._2._2.getOrElse(Iterator.empty)
            .foreach(y => {
              val c = y.cor.toCharArray
              corSeq = corSeq.updated(y.errPos, c(0))
            })
          (x._1, x._2._1.readName.get, corSeq, x._2._1.qual.get, x._2._1.readInFragment)
        })
      .map( // follow implementation of org.bdgenomics.adam.converters.makeAlignmentRecord
        itr => {
          AlignmentRecord.newBuilder
            .setReadName(itr._2)
            .setSequence(itr._3)
            .setQual(itr._4)
            .setReadPaired(true)
            .setReadInFragment(itr._5.get)
            .build
        })

    val noErrRdd = rdd.filter(_._2._2.isEmpty)
      .map( // follow implementation of org.bdgenomics.adam.converters.makeAlignmentRecord
        itr => {
          AlignmentRecord.newBuilder
            .setReadName(itr._2._1.readName.get)
            .setSequence(itr._2._1.sequence.get)
            .setQual(itr._2._1.qual.get)
            .setReadPaired(true)
            .setReadInFragment(itr._2._1.readInFragment.get)
            .build
        })

    if (seperateErr) {
      val _cor = "COR"
      val _noErr = "NO.ERR"

      AlignmentRecordRDD(correctedRdd, SequenceDictionary.empty, RecordGroupDictionary.empty, Seq.empty)
        .saveAsParquet(
          Paths.get(outPath, _cor).toString,
          128 * 1024 * 1024,
          1 * 1024 * 1024,
          CompressionCodecName.SNAPPY)

      AlignmentRecordRDD(noErrRdd, SequenceDictionary.empty, RecordGroupDictionary.empty, Seq.empty)
        .saveAsParquet(
          Paths.get(outPath, _noErr).toString,
          128 * 1024 * 1024,
          1 * 1024 * 1024,
          CompressionCodecName.SNAPPY)
    } else {
      AlignmentRecordRDD(correctedRdd union noErrRdd, SequenceDictionary.empty, RecordGroupDictionary.empty, Seq.empty)
        .saveAsParquet(
          outPath.toString,
          128 * 1024 * 1024,
          1 * 1024 * 1024,
          CompressionCodecName.SNAPPY)
    }
  }

  private def loadNumberTaggedReads(spark: SparkSession,
                                    path: String, columnNames: Seq[String],
                                    refReadMask: Long = 0x2000000000000000L,
                                    ref_flag: Boolean = false): Dataset[NumberedReads] = {
    import spark.implicits._
    spark.read.option("mergeSchema", "false")
      .parquet(path)
      .select(columnNames.head, columnNames.tail: _*)
      .map(x => (x.getAs[String](0), x.getAs[String](1), x.getAs[String](2)))
      .mapPartitions(iter => {
        iter.map(content => {
          // assign entries from reference samples with number of _refMask
          val number = {
            if (!ref_flag){
              val (_, iw) = AtgxReadsInfoParser.parseFromName(content._1)
              iw.getID
            }
            else refReadMask
          }
          NumberedReads(number, content._2, content._3)
        })
      })
  }

  private def buildSA(numberedReads: Dataset[NumberedReads], pfDir: String, batch: Int): Dataset[SuffixArray] = {
    val padBatch = "%07d".format(batch)
    val num_extra_partitions = SuffixArray.getExtraPartitions(args.pl, batch)
    val padPart = "%07d".format(TaskContext.getPartitionId())
    val t1 = System.currentTimeMillis()

    val ret = numberedReads
      .mapPartitions(NumberedReads.makeReverseComplement(_, rcMask))
      .mapPartitions(i => SuffixArray
        .build(i, args.pl, args.pl2, args.minlcp, args.maxrlen, batch,
          args.packing_size, num_extra_partitions, nameBitLen)
      )
      .rdd
      .partitionBy(new KeyPartitioner3(args.pl2, num_extra_partitions))
      .map(v => (v._2.cnt, v._2.readId, v._2.seq, v._2.suffixLength))

    if (args.profiling)
      writeProfilingInfo(
        s"$padBatch\t$padPart\tBuild SA with size ${ret.count()} Runtime: ${System.currentTimeMillis() - t1} ms\n",
        Paths.get(pfDir, "profiling", s"$padBatch-profiling-buildSA").toString)
    ret.toDS()
  }

  private def generateSS(sa: Iterator[SuffixArray], pfDir: String, batch: Int):
  Iterator[TwoBitIndexedSeq] = {
    val padBatch = "%07d".format(batch)
    val pfc = new StringBuilder
    val padPart = "%07d".format(TaskContext.getPartitionId())
    val rbl = (args.maxrlen / 4) + 2 //reformat bytearray by (suffix + prefix) with byte-alignment

    var t1 = System.currentTimeMillis()
    val tmp = (sa flatMap (x => {
      val ssa = ArrayBuffer.empty[TwoBitIndexedSeq]
      for (k <- 0 until x._1) {
        ssa += TwoBitIndexedSeq(
          seq = x._3,
          offset = (k * rbl).toShort, //NOTE: the position of the complete read
          length = x._4(k),
          idx = ((x._2(k) & offsetMask) >> nameBitLen).toShort,
          pos = ArrayBuffer[Long](x._2(k))
        )
      }
      ssa
    }))
      .toArray

    val inputSuffixCount = tmp.length
    pfc.append(s"$padBatch\t$padPart\tCreate TwoBitSufSeq5 with size: $inputSuffixCount Runtime: ${System.currentTimeMillis() - t1} ms\n")

    // sort TwoBitSufSeq
    t1 = System.currentTimeMillis()
    val ret = tmp.sortWith(SuffixTree.compareSS)

    pfc.append(s"$padBatch\t$padPart\tSort TwoBitSufSeq5 Runtime: $inputSuffixCount Runtime: ${System.currentTimeMillis() - t1} ms\n")

    if (args.profiling)
      writeProfilingInfo(
        pfc.toString,
        Paths.get(pfDir, "profiling", s"$padBatch-$padPart-profiling-generateSS").toString)
    ret.iterator
  }

  private def generateST(sss: Iterator[TwoBitIndexedSeq], pfDir: String, batch: Int): Iterator[ErrorEntry] = {
    val tid = TaskContext.getPartitionId()
    val padBatch = "%07d".format(batch)
    val padPart = "%07d".format(tid)

    // generate lcp
    var t1 = System.currentTimeMillis()
    val lcp = SuffixTree
      .indexSuffixArrayAndSort(sss, (args.pl + args.pl2).toShort)

    val readIDMatrix = Array.ofDim[ArrayBuffer[Long]](args.maxrlen, 5)
  
    // initialize readIDMatrix
    for (i <- 0 until args.maxrlen)
      for (j <- 0 to 4)
        readIDMatrix(i)(j) = ArrayBuffer.empty[Long]
    
    // update pa and do error correction
    t1 = System.currentTimeMillis()
    val ret = SuffixTree
      .indexLCPArray(
        lcp,
        (args.pl + args.pl2).toShort,
        args.maxrlen.toShort,
        correctError(readIDMatrix)
      )

    if (args.profiling)
      writeProfilingInfo(
        s"$padBatch\t$padPart\tCompute index Runtime: ${System.currentTimeMillis() - t1} ms\n",
        Paths.get(pfDir, "profiling", s"$padBatch-$padPart-profiling-generateST-03-indexIS").toString)

    ret
  }

  private def correctError(readIDMatrix: Array[Array[ArrayBuffer[Long]]])
                  (lb: Iterator[TwoBitIndexedSeq], ctable: Array[Array[Int]], dir: SuffixTree.TraverseDirection.Value):
  Option[Iterator[com.atgenomix.connectedreads.pipeline.ErrorEntry]] = {
    val errReport = ArrayBuffer.empty[ErrorEntry]
    
    def posEncode(entry: TwoBitIndexedSeq, rcMask: Long, offsetMask: Long, nameMask: Long, nameBitLen: Int): ArrayBuffer[Long] = {
      entry.pos
        .withFilter(x => (x & refReadMask) == 0) // filter out pooled refReads
        .map(x => {
          if ((x & rcMask) == 0) {
            (x & (rcMask + nameMask)) + ((((x & offsetMask) >> nameBitLen) + entry.p1) << nameBitLen)
          }
          else if (entry.length == entry.p1) {
            // for case p1 == length
            x & (rcMask + nameMask)
          }
          else {
            // for general case, update idx as entry.length - 1 - entry.idx
            (x & (rcMask + nameMask)) + ((entry.length - 1 - entry.p1).toLong << nameBitLen)
          }
        })
    }

    def findError(entry: TwoBitIndexedSeq): Unit = {
      val branchArray = ctable(entry.p1).take(4)
      // error site check
      if (entry.p1 >= args.minSufTreeDepth && branchArray.sum >= args.minreadsupport) {
        branchArray
          .view
          .zipWithIndex
          .withFilter(_._1 <= args.maxerrread)
          .withFilter(_._1 != 0)
          .foreach(eb => {
            branchArray
              .view
              .zipWithIndex
              .withFilter(_._1 > args.maxerrread)
              .foreach(cb => {
                if (branchArray(eb._2).toDouble / branchArray(cb._2) <= args.maxcorratio) {
                  val err: Char = (eb._2: @switch) match {
                    case 0 => 'A'
                    case 1 => 'C'
                    case 2 => 'G'
                    case 3 => 'T'
                  }
                  val compErr: Char = (eb._2: @switch) match {
                    case 0 => 'T'
                    case 1 => 'G'
                    case 2 => 'C'
                    case 3 => 'A'
                  }
                  val cor: Char = (cb._2: @switch) match {
                    case 0 => 'A'
                    case 1 => 'C'
                    case 2 => 'G'
                    case 3 => 'T'
                  }
                  val compCor: Char = (cb._2: @switch) match {
                    case 0 => 'T'
                    case 1 => 'G'
                    case 2 => 'C'
                    case 3 => 'A'
                  }
                  // report error only for reads from INPUT sample
                  errReport.appendAll(
                    readIDMatrix(entry.p1).take(4)(eb._2)
                      .flatMap(x => {
                        if ((x & refReadMask) == 0) {
                          if ((x & rcMask) == 0) {
                            Some(ErrorEntry(
                              readID = x & nameMask,
                              rc = false,
                              errPos = ((x & offsetMask) >> nameBitLen).toInt,
                              depth = entry.p1,
                              err = err.toString,
                              cor = cor.toString))
                          }
                          else {
                            Some(ErrorEntry(
                              readID = x & nameMask,
                              rc = true,
                              errPos = ((x & offsetMask) >> nameBitLen).toInt,
                              depth = entry.p1,
                              err = compErr.toString,
                              cor = compCor.toString))
                          }
                        }
                        else {
                          None
                        }
                      })
                  )
                }
              })
          })
      }
    }

    def updatePaPos(entry: TwoBitIndexedSeq): Array[Long] = {
      readIDMatrix(entry.p1)
        .flatMap(a => {
          a.map(r => {
            if ((r & rcMask) == 0)
              (r & (rcMask + nameMask)) + ((((r & offsetMask) >> nameBitLen) - entry.p1 + entry.pa) << nameBitLen)
            else
              (r & (rcMask + nameMask)) + ((((r & offsetMask) >> nameBitLen) + entry.p1 - entry.pa) << nameBitLen)
          })
        })
    }

    var entry: TwoBitIndexedSeq = null
    lb foreach (x => {
      if (entry == null) {
        entry = x
      }
      if (x.lp) {
        val branch = x.codePointAt(x.p1)
        if (ctable(x.p1)(branch) <= args.maxerrread)
          readIDMatrix(x.p1)(branch).++=(posEncode(x, rcMask, offsetMask, nameMask, nameBitLen))
        else
          readIDMatrix(x.p1)(branch).clear()
      }
    })

    findError(entry)

    // propagate readID at p1 level to pa level
    if (ctable(entry.p1).sum <= args.maxerrread) {
      assert(readIDMatrix(entry.pa)(entry.codePointAt(entry.pa)).isEmpty)
      readIDMatrix(entry.pa)(entry.codePointAt(entry.pa)).appendAll(updatePaPos(entry))
    }
    
    // flush readIDMatrix(p1)
    for (j <- 0 to 3)
      readIDMatrix(entry.p1)(j).clear()

    Some(errReport.iterator)
  }

  private def readLevelCollapse(errorRow: (Any, Iterable[ErrorEntry])):
  mutable.HashMap[(Long, Int, String, String), (Array[Int], Array[Int])] = {
    val hm = mutable.HashMap.empty[(Long, Int, String, String), (Array[Int], Array[Int])]
    errorRow._2
      .groupBy(x => (x.readID, x.errPos, x.err, x.cor))
      .foreach(x => {
        val revDepth = ArrayBuffer.empty[Int]
        val fwdDepth = ArrayBuffer.empty[Int]
        
        x._2.foreach(y => {
          if (y.rc)
            revDepth += y.depth
          else
            fwdDepth += y.depth
        })
        
        hm.+=((x._1, (revDepth.toArray, fwdDepth.toArray)))
      })
    hm
  }

  private def readLevelGroup(hm: mutable.HashMap[(Long, Int, String, String), (Array[Int], Array[Int])],
                     groupLen: Int):
  ArrayBuffer[Array[((Long, Int, String, String), (Array[Int], Array[Int]))]] = {
    val errGroup = ArrayBuffer.empty[Array[((Long, Int, String, String), (Array[Int], Array[Int]))]]
    val sortedKey = hm.keySet.toArray.sortBy(_._2)
    var startPos = sortedKey.head._2
    val tmpArr = ArrayBuffer.empty[((Long, Int, String, String), (Array[Int], Array[Int]))]

    for (pos <- sortedKey) {
      if ((pos._2 - startPos) < groupLen)
        tmpArr.append((pos, hm(pos)))
      else {
        errGroup.append(tmpArr.toArray)
        tmpArr.clear()
        tmpArr.append((pos, hm(pos)))
        startPos = pos._2
      }
    }

    errGroup.append(tmpArr.toArray)
    errGroup
  }

  private def readLevelReport(errGroup: ArrayBuffer[Array[((Long, Int, String, String), (Array[Int], Array[Int]))]],
                              rawErrCutoff: Int):
  ArrayBuffer[ErrorEntry] = {
    errGroup
      .flatMap(gp => {
        var i = 1
        while (i < gp.length && gp(i)._1._2 == gp.head._1._2) i += 1
        if (i < gp.length) None
        else {
          val res = ArrayBuffer.empty[(Int, ErrorEntry)]
          gp.foreach(x => {
            val cnt = x._2._1.length + x._2._2.length
            if (cnt >= rawErrCutoff) {
              res.append((cnt, ErrorEntry(x._1._1, rc = false, x._1._2, -1, x._1._3, x._1._4)))
            }
          })
          res
        }
      })
      .sortBy(- _._1)
      .take(1)
      .map(x => x._2)
  }

  private def writeProfilingInfo(c: String, path: String): Unit = {
    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)

    val profileF = new BufferedWriter(new OutputStreamWriter(
      fs.create(new Path(path), true)))
    profileF.write(c)
    profileF.close()
  }
}
