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

package com.atgenomix.connectedreads.io

import com.atgenomix.connectedreads.core.model.TwoBitIndexedSeq
import com.atgenomix.connectedreads.core.util.ArrayByteUtils._
import com.atgenomix.connectedreads.formats.avro.SuffixIndex
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


final class SuffixTreeReader(spark: SparkSession)
  extends AnyRef {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.avro.compression.codec", "snappy")
  
  private def makeTBISRecord(offset: Int, length: Int, pos: mutable.Buffer[Long],
                             p1: Int, pa: Int, lp: Boolean, next: Int):
  TwoBitIndexedSeq = {
    TwoBitIndexedSeq(
      offset = offset.toShort,
      length = length.toShort,
      pos = ArrayBuffer(pos.toArray: _*),
      p1 = p1.toShort,
      pa = pa.toShort,
      lp = lp,
      next = next.toByte
    )
  }
  
  private def fromAvroSuffixIndex(iter: Iterator[SuffixIndex]):
  Iterator[TwoBitIndexedSeq] = {
    val buf = ArrayBuffer.empty[Byte]
    var len = 0 // the actual 2-bit sequence length
    var pre: TwoBitIndexedSeq = null
    val out = iter map (x => {
      var cur: TwoBitIndexedSeq = null
      if (x.getParentBp == 0) {
        //=====================================================================
        // new branch from the root, always append at byte boundary
        //=====================================================================
        cur = makeTBISRecord(
          buf.length * 4,
          x.getLength,
          x.getPosition.asScala.asInstanceOf,
          x.getChildBp,
          0,
          x.getIsLeaf,
          x.getNext
        )
    
        append2BitByteArray(buf, buf.length * 4, x.getSequence.array, 0, x.getLength)
        len = buf.length * 4 + x.getLength
      }
      else if (x.getParentBp == pre.pa) {
        //=====================================================================
        // same parent node with different child branch
        //=====================================================================
        cur = makeTBISRecord(
          buf.length * 4,
          pre.pa + x.getLength,
          x.getPosition.asScala.asInstanceOf,
          x.getChildBp,
          x.getParentBp,
          x.getIsLeaf,
          x.getNext.toByte
        )
    
        append2BitByteArray(buf, buf.length * 4, buf.toArray, pre.offset, pre.pa)
        len = buf.length * 4 + pre.pa
    
        append2BitByteArray(buf, len, x.getSequence.array, 0, x.getLength)
        len = len + x.getLength
      }
      else if (x.getParentBp > pre.pa) {
        //=====================================================================
        //  change subtree downward, i.e. new subtree branch is seen.
        //=====================================================================
        cur = makeTBISRecord(
          pre.offset,
          x.getParentBp + x.getLength,
          x.getPosition.asScala.asInstanceOf,
          x.getChildBp,
          x.getParentBp,
          x.getIsLeaf,
          x.getNext
        )
    
        append2BitByteArray(buf, len, x.getSequence.array, 0, x.getLength)
        len = len + x.getLength
      }
      else if (x.getParentBp < pre.pa) {
        //=====================================================================
        // change subtree upward, the current subtree is done traversal
        //=====================================================================
        if (x.getParentBp == 0) {
          cur = makeTBISRecord(
            buf.length * 4,
            x.getLength,
            x.getPosition.asScala.asInstanceOf,
            x.getChildBp,
            0,
            x.getIsLeaf,
            x.getNext
          )
      
          append2BitByteArray(buf, buf.length * 4, x.getSequence.array, 0, x.getLength)
          len = buf.length * 4 + x.getLength
        }
        else {
          cur = makeTBISRecord(
            buf.length * 4,
            x.getParentBp + x.getLength,
            x.getPosition.asScala.asInstanceOf,
            x.getChildBp,
            x.getParentBp,
            x.getIsLeaf,
            x.getNext
          )
      
          append2BitByteArray(buf, buf.length * 4, buf.toArray, pre.offset, x.getParentBp)
          len = buf.length * 4 + x.getParentBp
      
          append2BitByteArray(buf, len, x.getSequence.array, 0, x.getLength)
          len = len + x.getLength
        }
      }
  
      pre = cur
      cur
    })
    
    // re-assign seq array
    val arr = buf.toArray
    out map (x => {
      x.seq = arr
      x
    })
  }
  
  /**
    * Load the content of the TwoBitIndexedSeq dataset in avro format at the specified path.
    *
    * @param path The hdfs path to load the content
    */
  def avro(path: String): DataFrame = {
    spark
      .read
      .format("com.databricks.spark.avro")
      .option("avroSchema", SuffixIndex.getClassSchema.toString)
      .load(path)
      .as[SuffixIndex](Encoders.kryo[SuffixIndex])
      .mapPartitions[TwoBitIndexedSeq](fromAvroSuffixIndex(_))
      .toDF()
  }
}
