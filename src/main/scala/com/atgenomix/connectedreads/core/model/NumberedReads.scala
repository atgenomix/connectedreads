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

package com.atgenomix.connectedreads.core.model

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.atgenomix.connectedreads.core.util.SequenceUtils._

object NumberedReads extends Serializable {
  
  /**
    * assign serial number to reads
    * @param reads
    * @param numbering
    * @return
    */
  def normalize(reads: DataFrame, numbering: Boolean, sharding_size: Int, spark: SparkSession):
  Dataset[NumberedReads] = {
    import spark.implicits._
    reads mapPartitions (iter => {
      var sn = TaskContext.getPartitionId() * sharding_size - 1
      iter map (r => {
        if (numbering) {
          sn += 1
          NumberedReads(sn, r.getString(1), r.getString(2))
        }
        else {
          NumberedReads(r.getString(0).split(" ").last.toLong, r.getString(1), r.getString(2))
        }
      })
    })
  }
  
  def makeReverseComplement(iter: Iterator[NumberedReads], mask: Long):
  Iterator[NumberedReads] = {
    // store reads ID and a flag of WC reverse complement in each record.
    iter flatMap (r => {
      Array[NumberedReads](r, NumberedReads(r.sn | mask, sequence = reverseComplementary(r.sequence), qual = r.qual.reverse))
    })
  }
}

case class NumberedReads(sn: Long, sequence: String, qual: String)
