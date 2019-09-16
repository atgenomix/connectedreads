/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.atgenomix.connectedreads.formats.avro;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
/** Record for describing suffix tree index, the data structure for
 parallel processing suffix tree. */
@org.apache.avro.specific.AvroGenerated
public class SuffixIndex extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3113618833378175648L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SuffixIndex\",\"namespace\":\"com.atgenomix.connectedreads.formats.avro\",\"doc\":\"Record for describing suffix tree index, the data structure for\\n parallel processing suffix tree.\",\"fields\":[{\"name\":\"prefix\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the common prefix this index belongs to. this is mostly used for data partitioning.\"},{\"name\":\"sequence\",\"type\":\"bytes\",\"doc\":\"two-bit encoded sequence data.\"},{\"name\":\"length\",\"type\":\"int\",\"doc\":\"the actual length of sequence.\"},{\"name\":\"parentBp\",\"type\":[\"int\",\"null\"],\"doc\":\"parent branching point in sequence, i.e. the second longest common prefix.\"},{\"name\":\"childBp\",\"type\":\"int\",\"doc\":\"child branching point in sequence, i.e. the first longest common prefix.\"},{\"name\":\"isLeaf\",\"type\":\"boolean\",\"doc\":\"true if this record also represents a leaf node.\"},{\"name\":\"next\",\"type\":[\"int\",\"null\"],\"doc\":\"bit-encoded next sequence alphabets at childBp.\\n   'A' => 0x01\\n   'C' => 0x02\\n   'G' => 0x04\\n   'N' => 0x08\\n   'T' => 0x10\\n   '$' => 0x20\"},{\"name\":\"position\",\"type\":[{\"type\":\"array\",\"items\":\"long\"},\"null\"],\"doc\":\"list of user-defined sequence positions.\",\"default\":[]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<SuffixIndex> ENCODER =
      new BinaryMessageEncoder<SuffixIndex>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<SuffixIndex> DECODER =
      new BinaryMessageDecoder<SuffixIndex>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<SuffixIndex> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<SuffixIndex> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<SuffixIndex>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this SuffixIndex to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a SuffixIndex from a ByteBuffer. */
  public static SuffixIndex fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** the common prefix this index belongs to. this is mostly used for data partitioning. */
  @Deprecated public java.lang.String prefix;
  /** two-bit encoded sequence data. */
  @Deprecated public java.nio.ByteBuffer sequence;
  /** the actual length of sequence. */
  @Deprecated public int length;
  /** parent branching point in sequence, i.e. the second longest common prefix. */
  @Deprecated public java.lang.Integer parentBp;
  /** child branching point in sequence, i.e. the first longest common prefix. */
  @Deprecated public int childBp;
  /** true if this record also represents a leaf node. */
  @Deprecated public boolean isLeaf;
  /** bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20 */
  @Deprecated public java.lang.Integer next;
  /** list of user-defined sequence positions. */
  @Deprecated public java.util.List<java.lang.Long> position;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public SuffixIndex() {}

  /**
   * All-args constructor.
   * @param prefix the common prefix this index belongs to. this is mostly used for data partitioning.
   * @param sequence two-bit encoded sequence data.
   * @param length the actual length of sequence.
   * @param parentBp parent branching point in sequence, i.e. the second longest common prefix.
   * @param childBp child branching point in sequence, i.e. the first longest common prefix.
   * @param isLeaf true if this record also represents a leaf node.
   * @param next bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
   * @param position list of user-defined sequence positions.
   */
  public SuffixIndex(java.lang.String prefix, java.nio.ByteBuffer sequence, java.lang.Integer length, java.lang.Integer parentBp, java.lang.Integer childBp, java.lang.Boolean isLeaf, java.lang.Integer next, java.util.List<java.lang.Long> position) {
    this.prefix = prefix;
    this.sequence = sequence;
    this.length = length;
    this.parentBp = parentBp;
    this.childBp = childBp;
    this.isLeaf = isLeaf;
    this.next = next;
    this.position = position;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return prefix;
    case 1: return sequence;
    case 2: return length;
    case 3: return parentBp;
    case 4: return childBp;
    case 5: return isLeaf;
    case 6: return next;
    case 7: return position;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: prefix = (java.lang.String)value$; break;
    case 1: sequence = (java.nio.ByteBuffer)value$; break;
    case 2: length = (java.lang.Integer)value$; break;
    case 3: parentBp = (java.lang.Integer)value$; break;
    case 4: childBp = (java.lang.Integer)value$; break;
    case 5: isLeaf = (java.lang.Boolean)value$; break;
    case 6: next = (java.lang.Integer)value$; break;
    case 7: position = (java.util.List<java.lang.Long>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'prefix' field.
   * @return the common prefix this index belongs to. this is mostly used for data partitioning.
   */
  public java.lang.String getPrefix() {
    return prefix;
  }

  /**
   * Sets the value of the 'prefix' field.
   * the common prefix this index belongs to. this is mostly used for data partitioning.
   * @param value the value to set.
   */
  public void setPrefix(java.lang.String value) {
    this.prefix = value;
  }

  /**
   * Gets the value of the 'sequence' field.
   * @return two-bit encoded sequence data.
   */
  public java.nio.ByteBuffer getSequence() {
    return sequence;
  }

  /**
   * Sets the value of the 'sequence' field.
   * two-bit encoded sequence data.
   * @param value the value to set.
   */
  public void setSequence(java.nio.ByteBuffer value) {
    this.sequence = value;
  }

  /**
   * Gets the value of the 'length' field.
   * @return the actual length of sequence.
   */
  public java.lang.Integer getLength() {
    return length;
  }

  /**
   * Sets the value of the 'length' field.
   * the actual length of sequence.
   * @param value the value to set.
   */
  public void setLength(java.lang.Integer value) {
    this.length = value;
  }

  /**
   * Gets the value of the 'parentBp' field.
   * @return parent branching point in sequence, i.e. the second longest common prefix.
   */
  public java.lang.Integer getParentBp() {
    return parentBp;
  }

  /**
   * Sets the value of the 'parentBp' field.
   * parent branching point in sequence, i.e. the second longest common prefix.
   * @param value the value to set.
   */
  public void setParentBp(java.lang.Integer value) {
    this.parentBp = value;
  }

  /**
   * Gets the value of the 'childBp' field.
   * @return child branching point in sequence, i.e. the first longest common prefix.
   */
  public java.lang.Integer getChildBp() {
    return childBp;
  }

  /**
   * Sets the value of the 'childBp' field.
   * child branching point in sequence, i.e. the first longest common prefix.
   * @param value the value to set.
   */
  public void setChildBp(java.lang.Integer value) {
    this.childBp = value;
  }

  /**
   * Gets the value of the 'isLeaf' field.
   * @return true if this record also represents a leaf node.
   */
  public java.lang.Boolean getIsLeaf() {
    return isLeaf;
  }

  /**
   * Sets the value of the 'isLeaf' field.
   * true if this record also represents a leaf node.
   * @param value the value to set.
   */
  public void setIsLeaf(java.lang.Boolean value) {
    this.isLeaf = value;
  }

  /**
   * Gets the value of the 'next' field.
   * @return bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
   */
  public java.lang.Integer getNext() {
    return next;
  }

  /**
   * Sets the value of the 'next' field.
   * bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
   * @param value the value to set.
   */
  public void setNext(java.lang.Integer value) {
    this.next = value;
  }

  /**
   * Gets the value of the 'position' field.
   * @return list of user-defined sequence positions.
   */
  public java.util.List<java.lang.Long> getPosition() {
    return position;
  }

  /**
   * Sets the value of the 'position' field.
   * list of user-defined sequence positions.
   * @param value the value to set.
   */
  public void setPosition(java.util.List<java.lang.Long> value) {
    this.position = value;
  }

  /**
   * Creates a new SuffixIndex RecordBuilder.
   * @return A new SuffixIndex RecordBuilder
   */
  public static com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder newBuilder() {
    return new com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder();
  }

  /**
   * Creates a new SuffixIndex RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new SuffixIndex RecordBuilder
   */
  public static com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder newBuilder(com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder other) {
    return new com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder(other);
  }

  /**
   * Creates a new SuffixIndex RecordBuilder by copying an existing SuffixIndex instance.
   * @param other The existing instance to copy.
   * @return A new SuffixIndex RecordBuilder
   */
  public static com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder newBuilder(com.atgenomix.connectedreads.formats.avro.SuffixIndex other) {
    return new com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder(other);
  }

  /**
   * RecordBuilder for SuffixIndex instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<SuffixIndex>
    implements org.apache.avro.data.RecordBuilder<SuffixIndex> {

    /** the common prefix this index belongs to. this is mostly used for data partitioning. */
    private java.lang.String prefix;
    /** two-bit encoded sequence data. */
    private java.nio.ByteBuffer sequence;
    /** the actual length of sequence. */
    private int length;
    /** parent branching point in sequence, i.e. the second longest common prefix. */
    private java.lang.Integer parentBp;
    /** child branching point in sequence, i.e. the first longest common prefix. */
    private int childBp;
    /** true if this record also represents a leaf node. */
    private boolean isLeaf;
    /** bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20 */
    private java.lang.Integer next;
    /** list of user-defined sequence positions. */
    private java.util.List<java.lang.Long> position;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.prefix)) {
        this.prefix = data().deepCopy(fields()[0].schema(), other.prefix);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.sequence)) {
        this.sequence = data().deepCopy(fields()[1].schema(), other.sequence);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.length)) {
        this.length = data().deepCopy(fields()[2].schema(), other.length);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.parentBp)) {
        this.parentBp = data().deepCopy(fields()[3].schema(), other.parentBp);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.childBp)) {
        this.childBp = data().deepCopy(fields()[4].schema(), other.childBp);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.isLeaf)) {
        this.isLeaf = data().deepCopy(fields()[5].schema(), other.isLeaf);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.next)) {
        this.next = data().deepCopy(fields()[6].schema(), other.next);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.position)) {
        this.position = data().deepCopy(fields()[7].schema(), other.position);
        fieldSetFlags()[7] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing SuffixIndex instance
     * @param other The existing instance to copy.
     */
    private Builder(com.atgenomix.connectedreads.formats.avro.SuffixIndex other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.prefix)) {
        this.prefix = data().deepCopy(fields()[0].schema(), other.prefix);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.sequence)) {
        this.sequence = data().deepCopy(fields()[1].schema(), other.sequence);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.length)) {
        this.length = data().deepCopy(fields()[2].schema(), other.length);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.parentBp)) {
        this.parentBp = data().deepCopy(fields()[3].schema(), other.parentBp);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.childBp)) {
        this.childBp = data().deepCopy(fields()[4].schema(), other.childBp);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.isLeaf)) {
        this.isLeaf = data().deepCopy(fields()[5].schema(), other.isLeaf);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.next)) {
        this.next = data().deepCopy(fields()[6].schema(), other.next);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.position)) {
        this.position = data().deepCopy(fields()[7].schema(), other.position);
        fieldSetFlags()[7] = true;
      }
    }

    /**
      * Gets the value of the 'prefix' field.
      * the common prefix this index belongs to. this is mostly used for data partitioning.
      * @return The value.
      */
    public java.lang.String getPrefix() {
      return prefix;
    }

    /**
      * Sets the value of the 'prefix' field.
      * the common prefix this index belongs to. this is mostly used for data partitioning.
      * @param value The value of 'prefix'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setPrefix(java.lang.String value) {
      validate(fields()[0], value);
      this.prefix = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'prefix' field has been set.
      * the common prefix this index belongs to. this is mostly used for data partitioning.
      * @return True if the 'prefix' field has been set, false otherwise.
      */
    public boolean hasPrefix() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'prefix' field.
      * the common prefix this index belongs to. this is mostly used for data partitioning.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearPrefix() {
      prefix = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'sequence' field.
      * two-bit encoded sequence data.
      * @return The value.
      */
    public java.nio.ByteBuffer getSequence() {
      return sequence;
    }

    /**
      * Sets the value of the 'sequence' field.
      * two-bit encoded sequence data.
      * @param value The value of 'sequence'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setSequence(java.nio.ByteBuffer value) {
      validate(fields()[1], value);
      this.sequence = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'sequence' field has been set.
      * two-bit encoded sequence data.
      * @return True if the 'sequence' field has been set, false otherwise.
      */
    public boolean hasSequence() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'sequence' field.
      * two-bit encoded sequence data.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearSequence() {
      sequence = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'length' field.
      * the actual length of sequence.
      * @return The value.
      */
    public java.lang.Integer getLength() {
      return length;
    }

    /**
      * Sets the value of the 'length' field.
      * the actual length of sequence.
      * @param value The value of 'length'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setLength(int value) {
      validate(fields()[2], value);
      this.length = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'length' field has been set.
      * the actual length of sequence.
      * @return True if the 'length' field has been set, false otherwise.
      */
    public boolean hasLength() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'length' field.
      * the actual length of sequence.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearLength() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'parentBp' field.
      * parent branching point in sequence, i.e. the second longest common prefix.
      * @return The value.
      */
    public java.lang.Integer getParentBp() {
      return parentBp;
    }

    /**
      * Sets the value of the 'parentBp' field.
      * parent branching point in sequence, i.e. the second longest common prefix.
      * @param value The value of 'parentBp'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setParentBp(java.lang.Integer value) {
      validate(fields()[3], value);
      this.parentBp = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'parentBp' field has been set.
      * parent branching point in sequence, i.e. the second longest common prefix.
      * @return True if the 'parentBp' field has been set, false otherwise.
      */
    public boolean hasParentBp() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'parentBp' field.
      * parent branching point in sequence, i.e. the second longest common prefix.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearParentBp() {
      parentBp = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'childBp' field.
      * child branching point in sequence, i.e. the first longest common prefix.
      * @return The value.
      */
    public java.lang.Integer getChildBp() {
      return childBp;
    }

    /**
      * Sets the value of the 'childBp' field.
      * child branching point in sequence, i.e. the first longest common prefix.
      * @param value The value of 'childBp'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setChildBp(int value) {
      validate(fields()[4], value);
      this.childBp = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'childBp' field has been set.
      * child branching point in sequence, i.e. the first longest common prefix.
      * @return True if the 'childBp' field has been set, false otherwise.
      */
    public boolean hasChildBp() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'childBp' field.
      * child branching point in sequence, i.e. the first longest common prefix.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearChildBp() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'isLeaf' field.
      * true if this record also represents a leaf node.
      * @return The value.
      */
    public java.lang.Boolean getIsLeaf() {
      return isLeaf;
    }

    /**
      * Sets the value of the 'isLeaf' field.
      * true if this record also represents a leaf node.
      * @param value The value of 'isLeaf'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setIsLeaf(boolean value) {
      validate(fields()[5], value);
      this.isLeaf = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'isLeaf' field has been set.
      * true if this record also represents a leaf node.
      * @return True if the 'isLeaf' field has been set, false otherwise.
      */
    public boolean hasIsLeaf() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'isLeaf' field.
      * true if this record also represents a leaf node.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearIsLeaf() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'next' field.
      * bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
      * @return The value.
      */
    public java.lang.Integer getNext() {
      return next;
    }

    /**
      * Sets the value of the 'next' field.
      * bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
      * @param value The value of 'next'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setNext(java.lang.Integer value) {
      validate(fields()[6], value);
      this.next = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'next' field has been set.
      * bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
      * @return True if the 'next' field has been set, false otherwise.
      */
    public boolean hasNext() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'next' field.
      * bit-encoded next sequence alphabets at childBp.
   'A' => 0x01
   'C' => 0x02
   'G' => 0x04
   'N' => 0x08
   'T' => 0x10
   '$' => 0x20
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearNext() {
      next = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'position' field.
      * list of user-defined sequence positions.
      * @return The value.
      */
    public java.util.List<java.lang.Long> getPosition() {
      return position;
    }

    /**
      * Sets the value of the 'position' field.
      * list of user-defined sequence positions.
      * @param value The value of 'position'.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder setPosition(java.util.List<java.lang.Long> value) {
      validate(fields()[7], value);
      this.position = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'position' field has been set.
      * list of user-defined sequence positions.
      * @return True if the 'position' field has been set, false otherwise.
      */
    public boolean hasPosition() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'position' field.
      * list of user-defined sequence positions.
      * @return This builder.
      */
    public com.atgenomix.connectedreads.formats.avro.SuffixIndex.Builder clearPosition() {
      position = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SuffixIndex build() {
      try {
        SuffixIndex record = new SuffixIndex();
        record.prefix = fieldSetFlags()[0] ? this.prefix : (java.lang.String) defaultValue(fields()[0]);
        record.sequence = fieldSetFlags()[1] ? this.sequence : (java.nio.ByteBuffer) defaultValue(fields()[1]);
        record.length = fieldSetFlags()[2] ? this.length : (java.lang.Integer) defaultValue(fields()[2]);
        record.parentBp = fieldSetFlags()[3] ? this.parentBp : (java.lang.Integer) defaultValue(fields()[3]);
        record.childBp = fieldSetFlags()[4] ? this.childBp : (java.lang.Integer) defaultValue(fields()[4]);
        record.isLeaf = fieldSetFlags()[5] ? this.isLeaf : (java.lang.Boolean) defaultValue(fields()[5]);
        record.next = fieldSetFlags()[6] ? this.next : (java.lang.Integer) defaultValue(fields()[6]);
        record.position = fieldSetFlags()[7] ? this.position : (java.util.List<java.lang.Long>) defaultValue(fields()[7]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<SuffixIndex>
    WRITER$ = (org.apache.avro.io.DatumWriter<SuffixIndex>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<SuffixIndex>
    READER$ = (org.apache.avro.io.DatumReader<SuffixIndex>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}