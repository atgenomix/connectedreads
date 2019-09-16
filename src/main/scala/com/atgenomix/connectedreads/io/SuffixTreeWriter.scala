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

import java.nio.ByteBuffer

import com.atgenomix.connectedreads.core.model.TwoBitIndexedSeq
import com.atgenomix.connectedreads.core.util.ArrayByteUtils._
import com.atgenomix.connectedreads.formats.avro.SuffixIndex
import com.databricks.spark.avro._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.collection.JavaConverters._


final case class SuffixTreeWriter (prefix_len: Short, spark: SparkSession)
  extends AnyRef {
  
  spark.conf.set("spark.sql.avro.compression.codec", "snappy")
  
  private def toAvroSuffixIndex(iter: Iterator[TwoBitIndexedSeq]):
  Iterator[SuffixIndex] = {
    iter map (i => {
      val o = new SuffixIndex()
      o.setPrefix(subdecode2bit(i.seq, i.offset, i.offset + prefix_len))
      o.setParentBp(Predef.int2Integer(i.pa))
      o.setChildBp(Predef.int2Integer(i.p1))
      o.setIsLeaf(Predef.boolean2Boolean(i.lp))
      o.setNext(Predef.int2Integer(i.next.toInt))
  
      if (i.pos.nonEmpty) {
        o.setPosition(i.pos.map(Predef.long2Long).asJava)
      }
  
      if (i.lp) {
        o.setSequence(ByteBuffer.wrap(sub2BitByteArray(i.seq, i.pa, i.length)))
        o.setLength(Predef.int2Integer(i.length - i.pa))
      }
      else {
        o.setSequence(ByteBuffer.wrap(sub2BitByteArray(i.seq, i.offset + i.pa, i.offset + i.p1)))
        o.setLength(Predef.int2Integer(i.p1 - i.pa))
      }
      o
    })
  }
  
  /**
    * Saves the content of the TwoBitIndexedSeq dataset in avro format at the specified path.
    *
    * @param path The hdfs path to save the content
    * @param df dataframe of TwoBitIndexedSeq objects
    */
  def avro(path: String, df: DataFrame): Unit = {
    import spark.implicits._
    
    df.as[TwoBitIndexedSeq]
      .mapPartitions(toAvroSuffixIndex)(Encoders.kryo[SuffixIndex])
      .write
      .option("avroSchema", SuffixIndex.getClassSchema.toString)
      .format("com.databricks.spark.avro")
      .partitionBy("prefix")
      .avro(path)
  }
}
