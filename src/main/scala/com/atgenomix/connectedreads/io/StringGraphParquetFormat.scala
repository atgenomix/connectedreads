package com.atgenomix.connectedreads.io

case class Vertex(ID: Long, seq: String, qual: String)

case class Edge(RP_ID: Long, RP_start: Int, RP_end: Int, RP_length: Int,
                RS_ID: Long, RS_start: Int, RS_end: Int, RS_length: Int,
                is_rc: Int, num_diff: Int)