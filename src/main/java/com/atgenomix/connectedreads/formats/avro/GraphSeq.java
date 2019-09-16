/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.atgenomix.connectedreads.formats.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface GraphSeq {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"GraphSeq\",\"namespace\":\"com.atgenomix.connectedreads.formats.avro\",\"types\":[{\"type\":\"record\",\"name\":\"SuffixIndex\",\"doc\":\"Record for describing suffix tree index, the data structure for\\n parallel processing suffix tree.\",\"fields\":[{\"name\":\"prefix\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the common prefix this index belongs to. this is mostly used for data partitioning.\"},{\"name\":\"sequence\",\"type\":\"bytes\",\"doc\":\"two-bit encoded sequence data.\"},{\"name\":\"length\",\"type\":\"int\",\"doc\":\"the actual length of sequence.\"},{\"name\":\"parentBp\",\"type\":[\"int\",\"null\"],\"doc\":\"parent branching point in sequence, i.e. the second longest common prefix.\"},{\"name\":\"childBp\",\"type\":\"int\",\"doc\":\"child branching point in sequence, i.e. the first longest common prefix.\"},{\"name\":\"isLeaf\",\"type\":\"boolean\",\"doc\":\"true if this record also represents a leaf node.\"},{\"name\":\"next\",\"type\":[\"int\",\"null\"],\"doc\":\"bit-encoded next sequence alphabets at childBp.\\n   'A' => 0x01\\n   'C' => 0x02\\n   'G' => 0x04\\n   'N' => 0x08\\n   'T' => 0x10\\n   '$' => 0x20\"},{\"name\":\"position\",\"type\":[{\"type\":\"array\",\"items\":\"long\"},\"null\"],\"doc\":\"list of user-defined sequence positions.\",\"default\":[]}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  public interface Callback extends GraphSeq {
    public static final org.apache.avro.Protocol PROTOCOL = com.atgenomix.connectedreads.formats.avro.GraphSeq.PROTOCOL;
  }
}