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

package com.atgenomix.connectedreads.core.util

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame.{SRC, DST}

object LabelUtils {
  final val BLOOM_FILTER_ENCODE_MASK = 0x000000FFFFFFFFFEL
  final val HUMAN_READABLE_OFFSET = 100000000L
  final val RC_MASK = 0x4000000000000000L
  // Column names used in GraphFrame
  final val IN  = "inDegree"
  final val OUT = "outDegree"
  // Column names used internally
  final val OVERLAP = "overlapLen"
  final val RANK = "rank"
  final val RC_ID = "rc_id"
  final val RC_SEQ = "rc_seq"
  final val SEQ = "seq"
  final val QUAL = "qual"
  final val START = "start"

  final val LABEL = "label"
  final val LABEL_S: Byte = 0x1
  final val LABEL_T: Byte  = 0x2
  final val LABEL_B: Byte  = 0x3
  final val LABEL_PS: Byte = 0x4
  final val LABEL_PT: Byte = 0x5
  final val LABEL_PT_PS: Byte = 0x6

  type VertexID = Long
  type RCVertexID = Long

  case class NormalizedEdge(src: Long,
    dst: Long,
    RS_start: Int,
    RS_end: Int,
    RS_length: Int,
    RP_start: Int,
    RP_end: Int,
    RP_length: Int,
    is_rc: Int
  ) {
    def toAssemblyEdge(): AssemblyEdge = {
      AssemblyEdge(src, dst, RS_end - RS_start + 1)
    }
  }

  case class AssemblyVertex(id: Long, seq: String, qual: String)
  case class AssemblyEdge(src: Long, dst: Long, overlapLen: Int)
  object AssemblyEdge {
    val columnOrder = Seq(SRC, DST, "overlapLen")
  }

  def splitColumn(df: DataFrame, oldColumnName: String, columnNames: List[String], pattern: String = "\\t| "): DataFrame = {
    val cols = columnNames.zipWithIndex.map{ case (name, idx) =>
      col("_tmp").getItem(idx).as(name)
    }
    df.withColumn("_tmp", split(col(oldColumnName), pattern))
      .select(cols: _*)
  }

  def modifyEdgeVertexID(edge: NormalizedEdge): AssemblyEdge = {
    val overlap = edge.RS_end - edge.RS_start + 1
    (edge.RS_start != 0, edge.RP_start == 0) match {
      // normal case
      // Type 1
      case (true, true) => {
        // Type 2 might be mis-identify as Type 1 if RP is totally overlap by RS(RS contains RP)
        if (edge.is_rc == 1) {
          AssemblyEdge(edge.src, edge.dst + RC_MASK, overlap)
        } else {
          AssemblyEdge(edge.src, edge.dst, overlap)
        }
      }
      // Type 2
      case (true, false) => {
        AssemblyEdge(edge.src, edge.dst + RC_MASK, overlap)
      }
      // Type 3
      case (false, true) => {
        AssemblyEdge(edge.src + RC_MASK, edge.dst, overlap)
      }
      // Type 4
      case (false, false) => {
        AssemblyEdge(edge.src + RC_MASK, edge.dst + RC_MASK, overlap)
      }
    }
  }

  def genReverseEdge(edge: AssemblyEdge): AssemblyEdge =  {
    AssemblyEdge(edge.dst ^ RC_MASK, edge.src ^ RC_MASK, edge.overlapLen)
  }
}
