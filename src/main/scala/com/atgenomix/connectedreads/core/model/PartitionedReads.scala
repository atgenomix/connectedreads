package com.atgenomix.connectedreads.core.model

/**
  * To leverage Kryo to improve shuffle throughput
  * Created by chungtsai_su on 2018/2/6.
  */
case class PartitionedReads (cnt: Int, readId: Array[Long], seq: Array[Byte], suffixLength: Array[Short])
