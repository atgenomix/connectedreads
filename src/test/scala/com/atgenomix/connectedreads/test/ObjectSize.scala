package com.atgenomix.connectedreads.test

import org.openjdk.jol.info.GraphLayout
import org.scalatest.{FlatSpec, Matchers}


/**
  * Created by chungtsai_su on 2017/1/10.
  */
class ObjectSize extends FlatSpec with Matchers {

    "xxxx" should "vvv" in {
      println(GraphLayout.parseInstance(GenomeSequence1(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence2(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence3(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence4(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence5(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence6(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence7(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence8(seq = Array[Byte](0))).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence9()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence10()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence11()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence12()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence13()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence14()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence15()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence16()).toFootprint)
      println(GraphLayout.parseInstance(GenomeSequence17()).toFootprint)

    }

}

/*
net.vartotal.seqml.GenomeSequence1@c730b35d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   [I
         1        48        48   net.vartotal.seqml.GenomeSequence1
         3                  88   (total)
 */
case class GenomeSequence1(
                            // the expanded sequence string
                            seq: Array[Byte],
  
                            offset: Short = -1,
  
                            // the position of the first longest common prefix with previous pair-wise sequence
                            var p1: Short = 0,
  
                            // the collection of positions which share the same sequence
                            var pos: Array[Int] = Array[Int](),
  
                            // the position of the second longest common prefix up to p1 with previous pair-wise sequence: p2 <= p1
                            var p2: Short = 0,
  
                            // the parent position of p1 in the final suffix tree, derived from p2
                            var pa: Short = -1,
  
                            // the grandparent position of p1 in the final suffix tree, derived from p2
                            // TODO actually we don't need this variable
                            //var gpa: Short = -1,
  
                            // flag indicating the subsequence following p1 is leaf edge in the final suffix tree, -1 => internal edge
                            var lp: Short = -1,
  
                            // all next characters immediately following p1
                            // A -> 1
                            // C -> 2
                            // G -> 4
                            // N -> 8
                            // T -> 16
                            // $ -> 32
                            var next: Byte = 0
                         )

/*
net.vartotal.seqml.GenomeSequence2@cad498cd footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   [I
         1        32        32   net.vartotal.seqml.GenomeSequence2
         3                  72   (total)
 */
case class GenomeSequence2(
                            // the expanded sequence string
                            seq: Array[Byte],

                            offset: Short = -1,

                            // the position of the first longest common prefix with previous pair-wise sequence
                            var p1: Short = 0,

                            // the position of the second longest common prefix up to p1 with previous pair-wise sequence: p2 <= p1
                            var p2: Short = 0,

                            // the parent position of p1 in the final suffix tree, derived from p2
                            var pa: Short = -1,

                            // the grandparent position of p1 in the final suffix tree, derived from p2
                            var gpa: Short = -1,

                            // flag indicating the subsequence following p1 is leaf edge in the final suffix tree, -1 => leaf edge
                            var lp: Boolean = true,

                            // the collection of positions which share the same sequence
                            var pos: Array[Int] = Array[Int]()
                          )

/*
net.vartotal.seqml.GenomeSequence3@6631f5cad footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   [I
         1        16        16   [S
         1        32        32   net.vartotal.seqml.GenomeSequence3
         4                  88   (total)
 */
case class GenomeSequence3(
                            // the expanded sequence string
                            seq: Array[Byte],

                            // the position of the first longest common prefix with previous pair-wise sequence
                            var idx: Array[Short] = Array[Short](),

                            // flag indicating the subsequence following p1 is leaf edge in the final suffix tree, -1 => leaf edge
                            var lp: Boolean = true,

                            // the collection of positions which share the same sequence
                            var pos: Array[Int] = Array[Int]()
                          )

/*
net.vartotal.seqml.GenomeSequence4@649bec2ed footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   [I
         1        16        16   [S
         1        24        24   net.vartotal.seqml.GenomeSequence4
         4                  80   (total)
 */
case class GenomeSequence4(
                            // the expanded sequence string
                            var seq: Array[Byte],

                            // the position of the first longest common prefix with previous pair-wise sequence
                            var idx: Array[Short] = Array[Short](),

                            // the collection of positions which share the same sequence
                            var pos: Array[Int] = Array[Int]()
                          )

/*
net.vartotal.seqml.GenomeSequence5@1cbb87f3d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         2        16        32   [I
         1        16        16   [S
         1        32        32   net.vartotal.seqml.GenomeSequence5
         5                 104   (total)
 */
case class GenomeSequence5(
                            // the expanded sequence string
                            var seq: Array[Byte],

                            // the position of the first longest common prefix with previous pair-wise sequence
                            var idx: Array[Short] = Array[Short](),

                            // the collection of positions which share the same sequence
                            var pos: Array[Int] = Array[Int](),

                            // the collection of positions which share the same sequence
                            var pos1: Array[Int] = Array[Int]()
                          )

/*
net.vartotal.seqml.GenomeSequence6@49912c99d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   [S
         1        24        24   net.vartotal.seqml.GenomeSequence6
         3                  64   (total)
 */
case class GenomeSequence6(
                            // the expanded sequence string
                            var seq: Array[Byte],

                            // the position of the first longest common prefix with previous pair-wise sequence
                            var idx: Array[Short] = Array[Short]()

                          )

/*
net.vartotal.seqml.GenomeSequence7@550ee7e5d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   net.vartotal.seqml.GenomeSequence7
         2                  40   (total)
*/
case class GenomeSequence7(
                            // the expanded sequence string
                            var seq: Array[Byte]

                          )

/*
net.vartotal.seqml.GenomeSequence8@48974e45d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   [B
         1        16        16   net.vartotal.seqml.GenomeSequence8
         2                  40   (total)
 */
case class GenomeSequence8(
                            // the expanded sequence string
                             seq: Array[Byte]

                          )

/*
net.vartotal.seqml.GenomeSequence9@50ad3bc1d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        16        16   [S
         1        16        16   net.vartotal.seqml.GenomeSequence9
         2                  32   (total)
 */
case class GenomeSequence9(
                            // the position of the first longest common prefix with previous pair-wise sequence
                            var idx: Array[Short] = Array[Short]()

                          )

/*
net.vartotal.seqml.GenomeSequence10@48974e45d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        32        32   net.vartotal.seqml.GenomeSequence10
         1                  32   (total)
 */
case class GenomeSequence10(
                             var p1: Short = 0,
                             var p2: Short = 0,
                             var p3: Short = 0,
                             var p4: Short = 0,
                             var p5: Short = 0,
                             var p6: Short = 0,
                             var p7: Short = 0,
                             var p8: Short = 0,
                             var p9: Short = 0,
                             var p10: Short = 0
                          )

/*
net.vartotal.seqml.GenomeSequence11@193f604ad footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   net.vartotal.seqml.GenomeSequence11
         1                  24   (total)
 */
case class GenomeSequence11(
                             offset: Short = -1,

                             // the position of the first longest common prefix with previous pair-wise sequence
                             var p1: Short = 0,

                             // the position of the second longest common prefix up to p1 with previous pair-wise sequence: p2 <= p1
                             var p2: Short = 0,

                             // the parent position of p1 in the final suffix tree, derived from p2
                             var pa: Short = -1,

                             // the grandparent position of p1 in the final suffix tree, derived from p2
                             var gpa: Short = -1
                           )

/*
net.vartotal.seqml.GenomeSequence12@57af006cd footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   net.vartotal.seqml.GenomeSequence12
         1                  24   (total)
 */
case class GenomeSequence12(
                             offset: Short = -1,

                             // the position of the first longest common prefix with previous pair-wise sequence
                             var p1: Short = 0,

                             // the position of the second longest common prefix up to p1 with previous pair-wise sequence: p2 <= p1
                             var p2: Short = 0,

                             // the parent position of p1 in the final suffix tree, derived from p2
                             var pa: Short = -1
                           )

/*
net.vartotal.seqml.GenomeSequence13@932bc4ad footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        24        24   net.vartotal.seqml.GenomeSequence13
         1                  24   (total)
 */
case class GenomeSequence13(
                             offset: Short = -1,

                             // the position of the first longest common prefix with previous pair-wise sequence
                             var p1: Short = 0,

                             // the position of the second longest common prefix up to p1 with previous pair-wise sequence: p2 <= p1
                             var p2: Short = 0
                           )

/*
net.vartotal.seqml.GenomeSequence14@2fd1433ed footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        16        16   net.vartotal.seqml.GenomeSequence14
         1                  16   (total)
 */
case class GenomeSequence14(
                             offset: Short = -1,

                             // the position of the first longest common prefix with previous pair-wise sequence
                             var p1: Short = 0
                           )
/*
net.vartotal.seqml.GenomeSequence15@3514a4c0d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        16        16   net.vartotal.seqml.GenomeSequence15
         1                  16   (total)
 */
case class GenomeSequence15(
                             offset: Short = -1
                           )
/*
net.vartotal.seqml.GenomeSequence16@50b5ac82d footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        16        16   net.vartotal.seqml.GenomeSequence16
         1                  16   (total)
 */
case class GenomeSequence16(
                             var offset: Short = -1
                           )
/*
net.vartotal.seqml.GenomeSequence17@6babf3bfd footprint:
     COUNT       AVG       SUM   DESCRIPTION
         1        16        16   net.vartotal.seqml.GenomeSequence17
         1                  16   (total)
 */
case class GenomeSequence17(

                           )

