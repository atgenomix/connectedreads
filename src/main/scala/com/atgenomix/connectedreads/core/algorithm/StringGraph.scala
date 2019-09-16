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

package com.atgenomix.connectedreads.core.algorithm

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.file.Paths
import java.util.UUID

import com.atgenomix.connectedreads.cli.StringGraphArgs
import com.atgenomix.connectedreads.core.model.{TwoBitPrefixSeq5, _}
import com.atgenomix.connectedreads.core.rdd.KeyPartitioner3
import com.atgenomix.connectedreads.core.util.AtgxReadsInfoParser
import com.atgenomix.connectedreads.io.{Edge, Vertex}
import com.atgenomix.connectedreads.core.util._
import htsjdk.samtools.util.BlockCompressedOutputStream
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class StringGraph(@transient val spark: SparkSession, args: StringGraphArgs,
                  nameBitLen: Int = 40,
                  nameMask: Long = 0x000000FFFFFFFFFFL,
                  offsetMask: Long = 0x00FFFF0000000000L,
                  rcMask: Long = 0x4000000000000000L) extends Serializable {

  import spark.implicits._
  val ID_SHARDING_SIZE = 2000000    //read naming by idx
  val MAX_PARTITIONS = 1000         // ID_SHARDING_SIZE * MAX_PARTITIONS < 2^31

  type SuffixMatrix = (Long, Seq[(Long, Long, TwoBitIndexedSeq, TwoBitIndexedSeq, Int, Int, Int)])
  type SuffixArray = (Int, Array[Long], Array[Byte], Array[Short])

  def preprocessing(batch: Int, numberedReads: Dataset[NumberedReads]): Dataset[SuffixArray] = {
    val num_extra_partitions = SuffixArray.getExtraPartitions(args.pl, batch)
    numberedReads
      .mapPartitions(NumberedReads.makeReverseComplement(_, rcMask))
      .mapPartitions(iter => SuffixArray
        .build(iter, args.pl, args.pl2, args.minlcp, args.maxrlen,
          batch, args.packing_size, num_extra_partitions, nameBitLen))
      .rdd.partitionBy(new KeyPartitioner3(args.pl2, num_extra_partitions))
      .map(v => (v._2.cnt, v._2.readId, v._2.seq, v._2.suffixLength))
      .toDS()
  }

  def findShortDupReads(s: Dataset[SuffixArray], batch: Int, statsPath: String): Dataset[NumberedReads] = {
      s.mapPartitions(
        mkSuffixMatrixForShortDupReads(_, batch, args.pl, args.pl2, args.minlcp, args.maxrlen, statsPath, args.stats, args.profiling))
  }

  def overlap(s: Dataset[SuffixArray], batch: Int, smout: String, tmp: String, statsPath: String): Unit = {
    if (args.outputFormat == "ASQG")
      s.foreachPartition(
        mkSuffixMatrix(_, batch, args.pl, args.pl2, args.minlcp, args.maxrlen, statsPath, args.stats, args.profiling,
          smout, tmp))
    else
      s.mapPartitions(
        mkSuffixMatrix(_, batch, args.pl, args.pl2, args.minlcp, args.maxrlen, statsPath, args.stats, args.profiling,
          smout, tmp))
        .write.parquet(smout)
  }

  def printstat(writer: BufferedWriter, flag: Boolean, content: String): Unit = {
    if (flag) {
      writer.write(content)
    }
  }

  def createFile(profiling: Boolean, stats: Boolean, statsPath: String, batchStr: String, pidStr: String):
  (BufferedWriter, BufferedWriter) = {
    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)

    val profileF = {
      if (profiling)
        new BufferedWriter(new OutputStreamWriter(
          fs.create(new Path(Paths.get(statsPath, "profiling", s"$batchStr-$pidStr-profiling").toString), true)))
      else
        null
    }
    val statsF = {
      if (stats)
        new BufferedWriter(new OutputStreamWriter(
          fs.create(new Path(Paths.get(statsPath, "stats", s"$batchStr-$pidStr-stats").toString), true)))
      else
        null
    }
    (profileF, statsF)
  }

  private def mkSuffixMatrix(iter: Iterator[SuffixArray], batch: Int, pl: Int, pl2: Int, minlcp: Int, maxrlen: Int,
                             statsPath: String, stats: Boolean, profiling: Boolean, smout: String, tmp: String):
  Iterator[Edge] = {

    val read_byte_length: Int = (maxrlen / 4) + 2 //reformat bytearray by (suffix + prefix) with byte-alignment
    val pid = TaskContext.getPartitionId()
    val padpid = "%07d".format(pid)
    val padbatch = "%07d".format(batch)
    val temp = ArrayBuffer[TwoBitSufSeq]()

    val p: Short = (pl + pl2).toShort

    val (profileF, statsF) = createFile(profiling, stats, statsPath, padbatch, padpid)

    var t1 = System.currentTimeMillis()
    while (iter.hasNext) {
      val elm = iter.next()
      for (k <- 0 until elm._1) { // e.g., k = 0 - 9
        temp += TwoBitSufSeq(
          seq = elm._3,
          offset = (k * read_byte_length).toShort, //NOTE: the position of the complete read
          length = elm._4(k),
          idx = ((elm._2(k) & offsetMask) >> nameBitLen).toShort,
          pos = elm._2(k)
        )
      }
    }

    val inputSuffixCount = temp.length
    var t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tCreate TwoBitSufSeq5 with size: $inputSuffixCount Runtime: ${t2 - t1} ms\n")

    // all records have same prefix, i.e., pl + pl2 => sort records
    t1 = System.currentTimeMillis()
    val sortedsa: Iterator[TwoBitSufSeq] = temp.toArray.sorted.iterator
    temp.clear()
    t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tSort TwoBitSufSeq5 Runtime: ${t2 - t1}ms\n")

    val seqHash = mutable.LongMap.empty[(Short, Array[Byte])] // [offset of bytearray, seq]

    // compute longest common prefix (LCP) of successive suffixes
    t1 = System.currentTimeMillis()
    val lcp = longestCommonPrefix(sortedsa, (pl + pl2).toShort, seqHash, maxrlen) //.toArray
    t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tCompute LCP array with size ${lcp.length} Runtime: ${t2 - t1}ms\n")

    //// sort suffixes by LCP pos < $ < alphabet.
    //t1 = System.currentTimeMillis()
    //val ss = lcp.toArray.sorted
    //t2 = System.currentTimeMillis()
    //printstat(profileF, profiling, s"$padbatch\t$padpid\tSort LCP array with size ${ss.length} Runtime: ${t2 - t1}ms\n")

    // suffix array => suffix matrix (SNj, SNi, Rj, Ri, dP - dS)
    t1 = System.currentTimeMillis()
    val edBuf = genSuffixMatrix(pid, lcp.iterator, seqHash, pl + pl2, maxrlen, smout, tmp)
    val edArray = edBuf.toArray
    val outputEdgesCount = edArray.length
    t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tCompute suffix matrix Runtime: ${t2 - t1}ms\n")

    printstat(statsF, stats, s"$padbatch\t$padpid\t$inputSuffixCount\t$outputEdgesCount\n")

    if (profiling) profileF.close()
    if (stats) statsF.close()

    edArray.iterator
  }

  private def mkSuffixMatrixForShortDupReads(iter: Iterator[SuffixArray], batch: Int, pl: Int, pl2: Int, minlcp: Int, maxrlen: Int,
                   statsPath: String, stats: Boolean, profiling: Boolean):
  Iterator[NumberedReads] = {

    val read_byte_length: Int = (maxrlen / 4) + 2 //reformat bytearray by (suffix + prefix) with byte-alignment
    val pid = TaskContext.getPartitionId()
    val padpid = "%07d".format(pid)
    val padbatch = "%07d".format(batch)
    val temp = ArrayBuffer[TwoBitSufSeq]()

    val p: Short = (pl + pl2).toShort

    val (profileF, statsF) = createFile(profiling, stats, statsPath, padbatch, padpid)

    var t1 = System.currentTimeMillis()
    while (iter.hasNext) {
      val elm = iter.next()
      for (k <- 0 until elm._1) { // e.g., k = 0 - 9
        temp += TwoBitSufSeq(
          seq = elm._3,
          offset = (k * read_byte_length).toShort, //NOTE: the position of the complete read
          length = elm._4(k),
          idx = ((elm._2(k) & offsetMask) >> nameBitLen).toShort,
          pos = elm._2(k)
        )
      }
    }

    val inputSuffixCount = temp.length
    var t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tShort duplicated reads: Create TwoBitSufSeq5 with size: $inputSuffixCount Runtime: ${t2 - t1} ms\n")

    // all records have same prefix, i.e., pl + pl2 => sort records
    t1 = System.currentTimeMillis()
    val sortedsa: Iterator[TwoBitSufSeq] = temp.toArray.sorted.iterator
    temp.clear()
    t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tShort duplicated reads: Sort TwoBitSufSeq5 Runtime: ${t2 - t1}ms\n")

    t1 = System.currentTimeMillis()
    // TODO: find short dup reads
    val vtBuf = genSuffixMatrixForShortDupReads(sortedsa, (pl + pl2).toShort, maxrlen)
    val vtArray = vtBuf.toArray
    val outputVtCount = vtArray.length
    t2 = System.currentTimeMillis()
    printstat(profileF, profiling, s"$padbatch\t$padpid\tShort duplicated reads: Compute suffix matrix Runtime: ${t2 - t1}ms\n")

    printstat(statsF, stats, s"$padbatch\t$padpid\t$inputSuffixCount\t$outputVtCount\n")

    if (profiling) profileF.close()
    if (stats) statsF.close()

    vtArray.iterator
  }

  // use pairwise compare to generate RS lists for each RP
  // result buffer looks like: [ RS-1.1, RS-1.2, RS-1.3, RP-1, RS-2.1, RS-2.2, RP-2, ... ]
  // basically, pairwise compare sorted suffix:
  // if 2 consecutive suffix have same sequence, merge these two together
  // if 2 consecutive suffix have different length, output the RSs and corresponding RP according to different case
  private def longestCommonPrefix(sorted: Iterator[TwoBitSufSeq], pl: Short,
                                  seqHash: mutable.LongMap[(Short, Array[Byte])],
                                  maxrlen: Int): ArrayBuffer[TwoBitIndexedSeq] = {
    val bucketlength = ((maxrlen / 4) + 2) * 4

    // note that the output entries must be RS or RP (check this constraint before append it)
    val buf = ArrayBuffer.empty[TwoBitIndexedSeq]
    var prev = if (sorted.hasNext) sorted.next else null
    var p1: Short = 0 // lcp
    // the following two variables used to identify the common ancestry (branch)
    var minlcp: Short = 0
    // temporary variables to keep RS and RP candidates when p1 = prev.length = curr.length (pos will be merged)
    var rs: TwoBitIndexedSeq = null // RS (read suffix), e.g., ________,$
    var rp: TwoBitIndexedSeq = null // RP (read prefix), i.e., offset = 0

    def mkSeq(): TwoBitIndexedSeq = {
      if (!seqHash.contains(prev.pos)) seqHash += ((prev.pos, (prev.offset, prev.seq)))

      TwoBitIndexedSeq(
        seq = prev.seq,
        offset = prev.offset,
        length = prev.length,
        idx = ((prev.pos & offsetMask) >> nameBitLen).toShort,
        p1 = p1,
        pa = minlcp,
        pos = ArrayBuffer(prev.pos)
      )
    }

    def flush(r: TwoBitIndexedSeq, seqHash: mutable.LongMap[(Short, Array[Byte])], mergepos: Boolean = true,
              reset: Boolean = true, isRS: Boolean = false) = {
      if (mergepos) {
        if (!seqHash.contains(prev.pos)) seqHash += ((prev.pos, (prev.offset, prev.seq)))
        r.pos += prev.pos
      }

      if (isRS) {
        r.p1 = r.length
      } else {
        r.p1 = p1
      }

      if (minlcp != 0) {
        r.pa = minlcp
        if (reset) minlcp = 0
      }

      buf += r
    }

    while (sorted.hasNext) {
      val curr = sorted.next()
      p1 = prev.commonPrefix(curr, pl) // i.e., lcp

      if (p1 == prev.length && prev.length == curr.length) {
        if (((prev.pos & offsetMask) >> nameBitLen) % bucketlength != 0) {
          if (rs == null) {
            rs = mkSeq()
          }
          else {
            if (!seqHash.contains(prev.pos)) seqHash += ((prev.pos, (prev.offset, prev.seq)))
            // if two consecutive suffix have identical suffix seq, then merge together
            // the only difference is pos
            rs.pos += prev.pos
          }
        } else {
          if (rp == null) {
            rp = mkSeq()
          }
          else {
            if (!seqHash.contains(prev.pos)) seqHash += ((prev.pos, (prev.offset, prev.seq)))
            rp.pos += prev.pos
          }
        }
      }
      else {
        if (p1 == prev.length && p1 >= args.minlcp) {
          // Candidate of RS
          if ((rs == null) && (rp == null)) {
            buf += mkSeq()
            minlcp = 0
          } else {
            // Case1: rs i-2 and i in rs; i-1 in rp
            //    ..........
            //    i-2 CCCC$  offset=2
            //    i-1 CCCC$  offset=0
            //    i   CCCC$  offset=2
            //    i+1 CCCCA$ offset=0
            if (((prev.pos & offsetMask) >> nameBitLen) % bucketlength != 0) {
              if (rs != null) {
                flush(rs, seqHash)
              } else {
                buf += mkSeq()
              }

              if (rp != null) {
                flush(rp, seqHash, mergepos = false)
              }
            } else {
              if (rs != null) {
                flush(rs, seqHash, mergepos = false, reset = false, isRS = true)
              }

              if (rp != null) {
                flush(rp, seqHash, reset = false)
              } else {
                buf += mkSeq()
              }
            }

            minlcp = 0
          }
        } else if (((prev.pos & offsetMask) >> nameBitLen) % bucketlength == 0) {
          // candidate of RP
          if (rs != null) {
            flush(rs, seqHash, mergepos = false, reset = false, isRS = true)
          }
          if (rp == null) {
            buf += mkSeq()
            minlcp = 0
          } else {
            // NOTE: prev should be added into rp if not empty(rp)
            // Case2: rs i-2 in rs; i-1 and i in rp
            //    ..........
            //    i-2 CCCC$  offset=2
            //    i-1 CCCC$  offset=0
            //    i   CCCC$  offset=0
            //    i+1 CCCT$  offset=5
            flush(rp, seqHash)
          }
        } else {
          if (rp != null) {
            // Case3: rs i-2 in rs; i-1 in rp
            //    ..........
            //    i-2 CCCC$  offset=2
            //    i-1 CCCC$  offset=0
            //    i   CCCC$  offset=2
            //    i+1 CCCT$  offset=5
            if (rs != null) {
              flush(rs, seqHash, mergepos = false, reset = false, isRS = true)
            }
            // why mergepos is false? because the RP is not empty and the current entry is not RP
            flush(rp, seqHash, mergepos = false)
          }
        }

        rs = null
        rp = null
      }

      if (minlcp == 0 || minlcp > p1) minlcp = p1

      prev = curr
    } // EOW

    // Note: for the last record, we don't need to consider RS because no RP will refer to it.
    // Therefore, we just need to handle RP case.
    if (prev != null) {
      if (((prev.pos & offsetMask) >> nameBitLen) % bucketlength == 0) {
        if (rs != null) {
          flush(rs, seqHash, reset = false, isRS = true)
        }
        if (rp == null) {
         p1 = (args.pl + args.pl2).toShort
          buf += mkSeq()
        } else {
          // Case 4: there are several same sequences in the last suffix list
          //    ..........
          //    N-3 TTTG$  offset=2
          //    N-2 TTTT$  offset=0
          //    N-1 TTTT$  offset=2
          //    N   TTTT$  offset=0
          flush(rp, seqHash)
        }
      } else if (rp != null) {
        // Case 5: even the last record is not RP, we should output those RP candidates if RP is not empty
        //    ..........
        //    N-3 TTTG$  offset=2
        //    N-2 TTTT$  offset=0
        //    N-1 TTTT$  offset=2
        //    N   TTTT$  offset=5
        flush(rp, seqHash, mergepos = false)
      }
    }

    buf
  }

  // assign serial number for reads
  def numbering(reads: DataFrame, minlcp: Int, numbering_enable: Boolean): Dataset[NumberedReads] = {
    if (numbering_enable) {
      val rs = if (reads.rdd.getNumPartitions > MAX_PARTITIONS) reads.repartition(MAX_PARTITIONS) else reads
      rs
        .map(x => x.getString(1) -> x.getString(2))
        .mapPartitions(iter => {
          val buf = new ArrayBuffer[NumberedReads]()
          var sn: Int = TaskContext.getPartitionId() * ID_SHARDING_SIZE
          while (iter.hasNext) {
            val content = iter.next()
            val seq = content._1
            val qual = content._2
            buf += NumberedReads(sn, seq, qual)
            sn = sn + 1
          }
          buf.iterator
        })
    } else {
      reads
        .map(x => (x.getString(0), x.getString(1), x.getString(2)))
        .mapPartitions(iter => {
          val buf = new ArrayBuffer[NumberedReads]()
          while (iter.hasNext) {
            val content = iter.next()
            val name = content._1
            val seq = content._2
            val qual = content._3
            val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
            buf += NumberedReads(iw.getID, seq, qual)
          }
          buf.iterator
        })
    }
  }

  private def genSuffixMatrix(pid: Int, iter: Iterator[TwoBitIndexedSeq], seqHash: mutable.LongMap[(Short, Array[Byte])],
                              prefix: Int, maxrlen: Int, smout: String, tmp: String):
  Iterator[Edge] = {
    val edBuf = ArrayBuffer[Edge]()
    val arr = iter.toArray

    val pid = TaskContext.getPartitionId()
    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"))
    val fs: FileSystem = FileSystem.get(conf)

    val (bw, tmp_path, dst_path) = {
      if (args.outputFormat == "ASQG") {
        val tmp_folder = new Path(tmp)
        val dst_folder = new Path(smout)
        val tmp_path = new Path(tmp + f"/$pid%05d.edges.gz_" + UUID.randomUUID())
        val dst_path = new Path(smout + f"/$pid%05d.edges.gz")
        fs.mkdirs(tmp_folder)
        fs.mkdirs(dst_folder)
        val bw = new BufferedWriter(
          new OutputStreamWriter(
            new BlockCompressedOutputStream(fs.create(tmp_path, true), null)
          )
        )
        (bw, tmp_path, dst_path)
      }

      else
        (null, null, null)
    }

    def find(idx: Int, mlcp: Int, maxrs: Int): ArrayBuffer[TwoBitPrefixSeq5] = {
      val buf = ArrayBuffer.empty[TwoBitPrefixSeq5]
      val rsIds = new LongOpenHashSet()
      val dbgIds = ArrayBuffer[Long]()
      var min_pa = if (arr(idx).pa == 0) Integer.MAX_VALUE else arr(idx).pa
      var j = idx - 1
      while (j >= 0) {
        // condition `arr(idx).length == arr(j).length` is for RSs which have same length as RP
        if ((arr(j).p1 == arr(j).length) && (min_pa >= arr(j).p1 || arr(idx).length == arr(j).length)) {
          for (pj <- arr(j).pos) {
            val this_rsid = pj & (nameMask + rcMask)
            if (!rsIds.contains(this_rsid) && buf.length < maxrs) {
              val s = seqHash(pj)
              buf += TwoBitPrefixSeq5(
                seq = s._2,
                offset = s._1.toShort,
                length = arr(j).length,
                idx = ((pj & offsetMask) >> nameBitLen).toShort,
                pos = pj
              )
              rsIds.add(this_rsid)
            }
          }
        }
        if ((min_pa > arr(j).pa) && (arr(j).pa > 0)) min_pa = arr(j).pa

        if (arr(j).pa < mlcp) j = -1 else j -= 1
        if (buf.length >= maxrs) j = -1
      }
      rsIds.clear()
      buf
    }

    def strncmp(prev: TwoBitPrefixSeq5, curr: TwoBitPrefixSeq5, len: Int): Int = {
      val min = scala.math.min(len, curr.idx)
      var res: Int = 0
      var i = 0
      val end = min / 4
      val prev_prefix_offset = prev.offset + (prev.length / 4) + 1
      val curr_prefix_offset = curr.offset + (curr.length / 4) + 1

      var break = false
      while (i < end && !break) {
        if (prev.seq(i + prev_prefix_offset) == curr.seq(i + curr_prefix_offset)) {
          i += 1
          res += 4
        } else {
          break = true
        }
      }
      if ((min % 4 != 0) || break) {
        val prev_prefix_start = ((prev.length / 4) + 1) * 4
        val curr_prefix_start = ((curr.length / 4) + 1) * 4
        while (res < min && prev.codePointAt(prev_prefix_start + res) == curr.codePointAt(curr_prefix_start + res)) res += 1
      }
      res
    }

    def addIrreducibleEdge(elm: TwoBitPrefixSeq5, idx: Int, idx_pos: Int, rsIds: LongOpenHashSet): Unit = {
      val rp_len = arr(idx).length
      val rs_len = elm.length + ((elm.pos & offsetMask) >> nameBitLen).toShort
      val s: Long = elm.pos & nameMask
      val sr: Long = elm.pos & (nameMask + rcMask)

      if (!rsIds.contains(sr)) {
        rsIds.add(sr)
        val pi = arr(idx).pos(idx_pos)
        val d: Long = pi & nameMask
        if (s < d) {
          if ((elm.pos & rcMask) == 0) {
            if ((pi & rcMask) == 0) {
              //+,+
              if (args.outputFormat == "ASQG")
                bw.write(s"ED\t$s $d ${rs_len - elm.length} ${rs_len - 1} $rs_len 0 ${elm.length - 1} $rp_len 0 0\n")
              else
                edBuf += Edge(d, 0, elm.length - 1, rp_len.toInt, s, rs_len - elm.length.toInt, rs_len - 1, rs_len, 0, 0)
            }
            else {
              //+,-
              if (args.outputFormat == "ASQG")
                bw.write(s"ED\t$s $d ${rs_len - elm.length} ${rs_len - 1} $rs_len ${rp_len - elm.length} ${rp_len - 1} $rp_len 1 0\n")
              else
                edBuf += Edge(d, rp_len - elm.length, rp_len - 1, rp_len, s, rs_len - elm.length.toInt, rs_len - 1, rs_len, 1, 0)
            }
          } else {
            if ((pi & rcMask) == 0) {
              //-,+
              if (args.outputFormat == "ASQG")
                bw.write(s"ED\t$s $d 0 ${elm.length - 1} $rs_len 0 ${elm.length - 1} $rp_len 1 0\n")
              else
                edBuf += Edge(d, 0, elm.length - 1, rp_len, s, 0, elm.length - 1, rs_len, 1, 0)
            }
            else {
              //-,-
              if (args.outputFormat == "ASQG")
                bw.write(s"ED\t$s $d 0 ${elm.length - 1} $rs_len ${rp_len - elm.length} ${rp_len - 1} $rp_len 0 0\n")
              else
                edBuf += Edge(d, rp_len - elm.length, rp_len - 1, rp_len, s, 0, elm.length - 1, rs_len, 0, 0)
            }
          }
        }
      }
    }

    def addIdenticalRead(idx: Int, rsIds: LongOpenHashSet): Unit = {
      val rp_len = arr(idx).length

      if (arr(idx).pos.length > 1) {
        for (i <- 0 to (arr(idx).pos.length - 2)) {
          val s: Long = arr(idx).pos(i) & nameMask
          if ((arr(idx).pos(i) & rcMask) == 0) {
            for (j <- (i + 1).until(arr(idx).pos.length)) {
              val d: Long = arr(idx).pos(j) & nameMask
              if (s < d) {
                if ((arr(idx).pos(j) & rcMask) == 0) {
                  if (args.outputFormat == "ASQG")
                    bw.write(s"ED\t$s $d 0 ${rp_len - 1} $rp_len 0 ${rp_len - 1} $rp_len 0 0\n")
                  else
                    edBuf += Edge(d, 0, rp_len - 1, rp_len, s, 0, rp_len - 1, rp_len, 0, 0)
                }
                else {
                  if (args.outputFormat == "ASQG")
                    bw.write(s"ED\t$s $d 0 ${rp_len - 1} $rp_len 0 ${rp_len - 1} $rp_len 1 0\n")
                  else
                    edBuf += Edge(d, 0, rp_len - 1, rp_len, s, 0, rp_len - 1, rp_len, 1, 0)
                }
              }
            }
          }
        }
      }
      for (i <- arr(idx).pos.indices) {
        rsIds.add(arr(idx).pos(i) & (nameMask + rcMask))
      }
    }

    def IdentifyIrreducibleEdges(prefix: ArrayBuffer[TwoBitPrefixSeq5], idx: Int): Unit = {
      // val rpIds = scala.collection.mutable.Set.empty[Long]
      val rpIds = new LongOpenHashSet()

      addIdenticalRead(idx, rpIds)

      if (prefix.nonEmpty) {
        val sortedPrefix = prefix.toArray.sorted
        for (k <- arr(idx).pos.indices) {
          val rsIds = rpIds.clone()
          var i = 0
          var prev = sortedPrefix(i)
          val that_rsid: Long = arr(idx).pos(k) & nameMask
          //identify the right first prefix (self should be excluded)
          while ((i < (sortedPrefix.length - 1)) && ((prev.pos & nameMask) == that_rsid)) {
            i += 1
            prev = sortedPrefix(i)
          }
          if ((i < sortedPrefix.length) && ((prev.pos & nameMask) != that_rsid)) {
            addIrreducibleEdge(prev, idx, k, rsIds)
            var pre_irr_len = prev.idx
            while (i < sortedPrefix.length) {
              if ((sortedPrefix(i).pos & nameMask) != that_rsid) {
                val curr = sortedPrefix(i)
                if (curr.idx < pre_irr_len) {
                  addIrreducibleEdge(curr, idx, k, rsIds)
                  pre_irr_len = curr.idx
                } else {
                  val l: Int = strncmp(prev, curr, pre_irr_len)
                  if (l != pre_irr_len) {
                    addIrreducibleEdge(curr, idx, k, rsIds)
                    pre_irr_len = curr.idx
                  } else if ((l == prev.idx) && (prev.idx == curr.idx) && (prev.length == curr.length)) {
                    addIrreducibleEdge(curr, idx, k, rsIds)
                    pre_irr_len = curr.idx
                  }
                }
                prev = curr
              }
              i += 1
            }
          }
        }
      }
    }

    // if the LCP of E is followed by an immediate $ then continue => RP only
    // the for loop iterates each RP
    // find() will iterate its corresponding RSs and pick RSs which validate the edge creation requirement
    for (id <- arr.length - 1 to 0 by -1; if arr(id).idx == 0) {
      val buf = find(id, args.minlcp, args.maxrs)
      IdentifyIrreducibleEdges(buf, id)
    }
    if (args.outputFormat == "ASQG") {
      bw.flush()
      bw.close()
      fs.rename(tmp_path, dst_path)
    }
    edBuf.iterator
  }

  private def genSuffixMatrixForShortDupReads(iter: Iterator[TwoBitSufSeq], prefix: Short, maxrlen: Int):
  Iterator[NumberedReads] = {
    val readBuf = ArrayBuffer[NumberedReads]()
    val arr = iter.toArray

    val conf = new Configuration
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"))

    for (id <- arr.length - 1 to 0 by -1; if arr(id).idx == 0) {
      // TODO: up: if (RS_suf_len == RP_suf_len && RP_suf_len == RP.p1 && RS.idx > 0) rm RP
      //       down: if(RS.p1 >= RP_suf_len) rm RP
      val up = if (id - 1 >= 0) Some(id - 1) else None
      val down = if (id + 1 <= arr.length - 1) Some(id + 1) else None

      up.foreach { u =>
        val p1 = arr(u).commonPrefix(arr(id), prefix)
        if (arr(u).length == arr(id).length && p1 == arr(id).length && arr(u).idx > 0) {
          // no need for qual because just want to record short dup reads
          readBuf += NumberedReads(arr(id).pos & nameMask, arr(id).fullDecode, "")
        } else {
          down.foreach { d =>
            val p1 = arr(d).commonPrefix(arr(id), prefix)
            if (p1 >= arr(id).length) {
              readBuf += NumberedReads(arr(id).pos & nameMask, arr(id).fullDecode, "")
            }
          }
        }
      }
    }

    readBuf.iterator
  }
}
