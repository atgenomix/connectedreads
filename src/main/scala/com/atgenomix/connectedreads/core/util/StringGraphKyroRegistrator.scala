package com.atgenomix.connectedreads.core.util

import com.esotericsoftware.kryo.io.{Input, KryoDataInput, KryoDataOutput, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.KryoRegistrator

/**
  * A Kryo serializer for Hadoop writables.
  *
  * Lifted from the Apache Spark user email list
  * (http://apache-spark-user-list.1001560.n3.nabble.com/Hadoop-Writable-and-Spark-serialization-td5721.html)
  * which indicates that it was originally copied from Shark itself, back when
  * Spark 0.9 was the state of the art.
  *
  * @tparam T The class to serialize, which implements the Writable interface.
  */
class WritableSerializer[T <: Writable] extends Serializer[T] {
  override def write(kryo: Kryo, output: Output, writable: T) {
    writable.write(new KryoDataOutput(output))
  }

  override def read(kryo: Kryo, input: Input, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    writable.readFields(new KryoDataInput(input))
    writable
  }
}


class StringGraphKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register Avro classes using fully qualified class names
    // Sort alphabetically and add blank lines between packages

    // java.lang
    kryo.register(classOf[java.lang.Class[_]])

    // java.util
    kryo.register(classOf[java.util.ArrayList[_]])
    kryo.register(classOf[java.util.LinkedHashMap[_, _]])
    kryo.register(classOf[java.util.LinkedHashSet[_]])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[java.util.HashSet[_]])

    // org.apache.avro
    kryo.register(Class.forName("org.apache.avro.Schema$RecordSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Field"))
    kryo.register(Class.forName("org.apache.avro.Schema$Field$Order"))
    kryo.register(Class.forName("org.apache.avro.Schema$UnionSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Type"))
    kryo.register(Class.forName("org.apache.avro.Schema$LockableArrayList"))
    kryo.register(Class.forName("org.apache.avro.Schema$BooleanSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$NullSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$StringSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$IntSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$FloatSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$EnumSchema"))
    kryo.register(Class.forName("org.apache.avro.Schema$Name"))
    kryo.register(Class.forName("org.apache.avro.Schema$LongSchema"))
    kryo.register(Class.forName("org.apache.avro.generic.GenericData$Array"))

    // org.apache.hadoop.conf
    kryo.register(classOf[org.apache.hadoop.conf.Configuration],
      new WritableSerializer[org.apache.hadoop.conf.Configuration])
    kryo.register(classOf[org.apache.hadoop.yarn.conf.YarnConfiguration],
      new WritableSerializer[org.apache.hadoop.yarn.conf.YarnConfiguration])

    // org.apache.hadoop.io
    kryo.register(classOf[org.apache.hadoop.io.Text])
    kryo.register(classOf[org.apache.hadoop.io.LongWritable])



    // org.codehaus.jackson.node
    kryo.register(classOf[org.codehaus.jackson.node.NullNode])
    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
    kryo.register(classOf[org.codehaus.jackson.node.TextNode])

    // org.apache.spark
    try {
      kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
      kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"))
    } catch {
      case cnfe: java.lang.ClassNotFoundException => {
        //log.info("Did not find Spark internal class. This is expected for Spark 1.")
      }
    }
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])
    kryo.register(Class.forName("org.apache.spark.sql.types.BooleanType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.FloatType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"))
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(classOf[org.apache.spark.sql.types.MapType])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])

    // scala
    kryo.register(classOf[scala.Array[scala.Array[Byte]]])
    kryo.register(classOf[scala.Array[htsjdk.variant.vcf.VCFHeader]])
    kryo.register(classOf[scala.Array[java.lang.Integer]])
    kryo.register(classOf[scala.Array[java.lang.Long]])
    kryo.register(classOf[scala.Array[java.lang.Object]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[scala.Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[scala.Array[scala.collection.Seq[_]]])
    kryo.register(classOf[scala.Array[Int]])
    kryo.register(classOf[scala.Array[Long]])
    kryo.register(classOf[scala.Array[String]])
    kryo.register(classOf[scala.Array[Option[_]]])
    kryo.register(Class.forName("scala.Tuple2$mcCC$sp"))

    // scala.collection
    kryo.register(Class.forName("scala.collection.Iterator$$anon$11"))
    kryo.register(Class.forName("scala.collection.Iterator$$anonfun$toStream$1"))

    // scala.collection.convert
    kryo.register(Class.forName("scala.collection.convert.Wrappers$"))

    // scala.collection.immutable
    kryo.register(classOf[scala.collection.immutable.::[_]])
    kryo.register(classOf[scala.collection.immutable.Range])
    kryo.register(Class.forName("scala.collection.immutable.Stream$Cons"))
    kryo.register(Class.forName("scala.collection.immutable.Stream$Empty$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))

    // scala.collection.mutable
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.collection.mutable.ListBuffer[_]])
    kryo.register(Class.forName("scala.collection.mutable.ListBuffer$$anon$1"))
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // scala.math
    kryo.register(scala.math.Numeric.LongIsIntegral.getClass)

    // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
    //
    //  https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
    //
    // See also:
    //
    //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))

    // needed for manifests
    kryo.register(Class.forName("scala.reflect.ManifestFactory$ClassTypeManifest"))

    // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[(Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])
    kryo.register(classOf[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]])

    kryo.register(Map.empty.getClass)
    kryo.register(Nil.getClass)
    kryo.register(None.getClass)


    // for Spark 2.2.1
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])

    //added for StringGraph
    kryo.register(classOf[scala.collection.mutable.ArraySeq[Any]])
    kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus"))
    kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation"))
    kryo.register(java.lang.reflect.Array.newInstance(Class.forName("org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation"), 0).getClass)
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))

    //kryo.register(classOf[Array[com.atgenomix.connectedreads.core.model.PartitionedReads]])
    kryo.register(classOf[com.atgenomix.connectedreads.core.model.PartitionedReads])

  }
}