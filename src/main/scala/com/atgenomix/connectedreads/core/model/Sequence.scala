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

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer

trait Sequence {
  def charAt(i: Int): Char

  def decode: String

  def codePointAt(i: Int): Int

  def commonPrefix(s: Sequence, skip: Short = 0): Short

  def subsequence(start: Int, end: Int): Array[Byte]

  def fullDecode: String

  def length: Short
}

abstract class TwoBitCompactSeq extends Sequence {
  var seq: Array[Byte]
  val offset: Short
  val length: Short
  
  override def decode: String = {
    val buf = new StringBuilder
    var i = 0
    while (i <= this.length) {
      buf += charAt(i)
      i += 1
    }
    buf.toString
  }

  override def charAt(i: Int): Char = {
    (this.codePointAt(i): @switch) match {
      case 0 => 'A'
      case 1 => 'C'
      case 2 => 'G'
      case 3 => 'T'
      case _ => '$'
    }
  }

  override def codePointAt(i: Int): Int = {
    var c = 4 // code point for $
    if (i != length) {
      val p = offset * 4 + i // byte-level to corresponding string-level
      // p & 0x03 equals p / 4
      c = (p & 0x03: @switch) match {
        case 0 => (seq(p >> 2) & 0xC0) >> 6
        case 1 => (seq(p >> 2) & 0x30) >> 4
        case 2 => (seq(p >> 2) & 0x0C) >> 2
        case 3 => (seq(p >> 2) & 0x03) >> 0
      }
    }
    c
  }

  override def subsequence(start: Int, end: Int): Array[Byte] = {
    var bytelen = (end - start) >> 2
    if ((end - start) % 4 != 0) bytelen += 1
    val buf = new Array[Byte](bytelen)
    var si = start
    var bi = 0
    while (si < end) {
      var c = this.codePointAt(si)
      if (c == 4) c = 0
      (bi & 0x03: @switch) match {
        case 0 => buf(bi >> 2) = (c << 6).toByte
        case 1 => buf(bi >> 2) = (buf(bi >> 2) | (c << 4)).toByte
        case 2 => buf(bi >> 2) = (buf(bi >> 2) | (c << 2)).toByte
        case 3 => buf(bi >> 2) = (buf(bi >> 2) | (c << 0)).toByte
      }
      si += 1
      bi += 1
    }
    buf
  }

  override def fullDecode: String = {
    val buf = new StringBuilder
    var i = offset.toInt
    val len = length >> 2
    while (i < offset + len) {
      buf += (intToChar((seq(i) & 0xC0) >> 6), intToChar((seq(i) & 0x30) >> 4), intToChar((seq(i) & 0x0C) >> 2), intToChar(seq(i) & 0x03))
      i = i + 1
    }
    // for special case when length of [B <= 4 or deal with the last byte within length bound
    var s = 4 * len
    while (s < length) {
      val c = (s & 0x03: @switch) match {
        case 0 => intToChar((seq(i) & 0xC0) >> 6)
        case 1 => intToChar((seq(i) & 0x30) >> 4)
        case 2 => intToChar((seq(i) & 0x0C) >> 2)
        case 3 => intToChar(seq(i) & 0x03)
      }
      buf += c
      s = s + 1
    }
    (buf += '$').toString
  }

  private[this] def intToChar(b: Int): Char =
    (b: @switch) match {
      case 0 => 'A'
      case 1 => 'C'
      case 2 => 'G'
      case 3 => 'T'
      case _ => throw new IllegalArgumentException(s"Unknown option: '$b'")
    }
}

abstract class FourBitCompactSeq extends Sequence {
  val seq: Array[Byte]
  val offset: Short

  override def length: Short = {
    val tail = if (((seq.last >> 4) & 0x0F) == 0) 1 else 2
    ((seq.length - 1) * 2 + tail).toShort
  }

  override def decode: String = {
    val buf = new StringBuilder
    val len = this.length
    var i = 0
    while (i < len) {
      buf += charAt(i)
      i += 1
    }
    buf.toString
  }

  override def charAt(i: Int): Char = {
    (this.codePointAt(i): @switch) match {
      case 0 => '$'
      case 1 => 'A'
      case 2 => 'C'
      case 3 => 'G'
      case 4 => 'N'
      case 5 => 'T'
      case _ => throw new IllegalArgumentException
    }
  }

  override def codePointAt(i: Int): Int = {
    val p = offset + i
    val c = (p & 0x01: @switch) match {
      case 0 => (seq(p >> 1) & 0xF0) >> 4
      case 1 => (seq(p >> 1) & 0x0F) >> 0
    }
    c
  }

  override def subsequence(start: Int, end: Int): Array[Byte] = {
    var bytelen = (end - start) >> 1
    if ((end - start) % 2 != 0) bytelen += 1
    val buf = new Array[Byte](bytelen)
    var si = start
    var bi = 0
    while (si < end) {
      val c = this.codePointAt(si)
      (bi & 0x01: @switch) match {
        case 0 => buf(bi >> 1) = (c << 4).toByte
        case 1 => buf(bi >> 1) = (buf(bi >> 1) | c).toByte
      }
      si += 1
      bi += 1
    }
    buf
  }

  override def fullDecode: String = {
    val buf = new StringBuilder
    for (i <- 0 until seq.length - 1) {
      buf += (intToChar((seq(i) & 0xf0) >> 4), intToChar(seq(i) & 0x0f)) // (left 4 bits, right 4 bits)
    }

    val tail = intToChar((seq(seq.length - 1) & 0xf0) >> 4)
    buf += tail
    if (tail != '$') buf += '$'

    buf.toString
  }

  private[this] def intToChar(b: Int): Char =
    (b: @switch) match {
      case 0 => '$'
      case 1 => 'A'
      case 2 => 'C'
      case 3 => 'G'
      case 4 => 'N'
      case 5 => 'T'
      case _ => throw new IllegalArgumentException(s"Unknown option: '$b'")
    }
}

// seq => packed seqs
case class TwoBitSufSeq(var seq: Array[Byte], offset: Short, length: Short, idx: Short, pos: Long)
  extends TwoBitCompactSeq with Ordered[TwoBitSufSeq] {

  override def compare(that: TwoBitSufSeq): Int = {
    val min: Int = if (this.length < that.length) this.length else that.length
    var res: Int = 0
    var i = 0
    val end = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < end && res == 0) {
      if (this.seq(i + this.offset) < that.seq(i + that.offset)) res = -1
      else if (this.seq(i + this.offset) > that.seq(i + that.offset)) res = 1
      else i += 1
    }

    // e.g., "AAA" vs. "AAAA", min = 3
    if (res == 0) {
      if (this.length < that.length) res = -1
      else if (this.length > that.length) res = 1
      else {
        if (this.idx < that.idx) {
          res = 1
        }
        else if (this.idx > that.idx) {
          res = -1
        }
        else {
          val this_rsid = this.pos & 0xFFFFFFFFL
          val that_rsid = that.pos & 0xFFFFFFFFL
          if (this_rsid < that_rsid) res = -1
          else if (this_rsid > that_rsid) res = 1
        }
      }
    } else {
      // need this check for fixing signed byte comparison
      if (((this.seq(i + this.offset) & 0x80) ^ (that.seq(i + that.offset) & 0x80)) != 0) res = -res
    }
    res
  }

  override def commonPrefix(s: Sequence, skip: Short = 0): Short = {
    val that = s.asInstanceOf[TwoBitSufSeq]
    val min = scala.math.min(this.length, that.length)
    var res: Int = min
    var i = skip / 4
    val end = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < end && (this.seq(i + this.offset) == that.seq(i + that.offset))) i += 1
    if (i < end) {
      res = i * 4
      while (res < min && this.codePointAt(res) == that.codePointAt(res)) res += 1
    }
    res.toShort
  }
}

case class TwoBitIndexedSeq(
                             var seq: Array[Byte] = null,
                             offset: Short = 0,
                             length: Short = 0,
                             idx: Short = 0,
                             pos: ArrayBuffer[Long],
                             var p1: Short = 0,
                             var pa: Short = -1,
                             var lp: Boolean = true, // once a leaf, always a leaf
                             var next: Byte = 0
                           )
  extends TwoBitCompactSeq with Ordered[TwoBitIndexedSeq] {

  override def compare(that: TwoBitIndexedSeq): Int = {
    val min: Int = if (this.length < that.length) this.length else that.length
    var res: Int = 0
    var i = 0
    val end: Int = min / 4
    while (i < end && res == 0) {
      if (this.seq(i + this.offset) < that.seq(i + that.offset)) res = -1
      else if (this.seq(i + this.offset) > that.seq(i + that.offset)) res = 1
      i += 1
    }

    if (res != 0) {
      if (((this.seq(i + this.offset - 1) & 0x80) ^ (that.seq(i + that.offset - 1) & 0x80)) != 0) res = -res
    }
    else if (min % 4 != 0) {
      i = i * 4
      while (i < min && res == 0) {
        val c1 = this.codePointAt(i)
        val c2 = that.codePointAt(i)
        if (c1 < c2) res = -1
        else if (c1 > c2) res = 1
        i += 1
      }
    }
    if (res == 0) {
      if (this.length < that.length) {
        res = -1
      } else if (this.length > that.length) {
        res = 1
      }
    }

    res
  }

  def commonPrefix(s: Sequence, skip: Short = 0): Short = {
    val that = s.asInstanceOf[TwoBitIndexedSeq]
    val min: Int = if (this.p1 < that.p1) this.p1 else that.p1
    var i: Int = skip
    while (i < min && this.codePointAt(i) == that.codePointAt(i)) i += 1
    i.toShort
  }

  // Assume the length of all reads are the same, so we can check the offset
  // of the first element of position in Rj
  //
  //                    80              140
  //                    |                |
  //  RS ---------------|----------------|,$
  //                    |================|==================== RP
  //                    0             <- 60
  //
  //  def isProperPrefix(that: Sequence): Boolean = {
  //    // cal. the index to trace the RS in backward direction
  //    // RS.L = RS.length - 1 + RS.offset (0xFFFF)
  //    // d = RS.L - RS.offset = RS.length - 1
  //    var d = this.length - 1 - 1 // 1: $, 1=> 0-base
  //    while (d >= 0 && this.codePointAt(d) == that.codePointAt(d)) d -= 1
  //    if (d < 0) true else false
  //  }
  def isProperPrefix(that: TwoBitIndexedSeq, prefix: Int): Boolean = {
    var res: Boolean = true
    val end: Int = this.length / 4

    // char-level
    var m = end * 4
    while (m < this.length && this.codePointAt(m) == that.codePointAt(m)) {
      m += 1
    }
    if (m < this.length) {
      res = false
    } else {
      // byte-level
      val min = (prefix / 4) - 1
      var i = end - 1
      while ((i > min) && (this.seq(i + this.offset) == that.seq(i + that.offset))) {
        i -= 1
      }
      if (i > min) res = false
    }

    res
  }
}

case class TwoBitPrefixSeq5(var seq: Array[Byte], offset: Short, length: Short, idx: Short, pos: Long)
  extends TwoBitCompactSeq with Ordered[TwoBitPrefixSeq5] {

  override def compare(that: TwoBitPrefixSeq5): Int = {
    val nameMask: Long = 0x000000FFFFFFFFFFL
    val rcMask: Long = 0x4000000000000000L

    val min: Int = if (this.idx < that.idx) this.idx else that.idx
    var res: Int = 0
    var i = 0
    val this_prefix_offset = this.offset + (this.length / 4) + 1
    val that_prefix_offset = that.offset + (that.length / 4) + 1

    val end = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < end && res == 0) {
      if (this.seq(i + this_prefix_offset) < that.seq(i + that_prefix_offset)) res = -1
      else if (this.seq(i + this_prefix_offset) > that.seq(i + that_prefix_offset)) res = 1
      i += 1
    }

    // e.g., "AAA" vs. "AAAA", min = 3
    if (res == 0) {
      if (this.idx < that.idx) {
        res = -1
      } else if (this.idx > that.idx) {
        res = 1
      } else {
        res =
          if (this.length > that.length) -1
          else if (this.length < that.length) 1
          else {
            val thisId = this.pos & (nameMask + rcMask)
            val thatId = that.pos & (nameMask + rcMask)

            if (thisId < thatId) -1 else 1
          }
      }
    } else {
      if (((this.seq(i + this_prefix_offset - 1) & 0x80) ^ (that.seq(i + that_prefix_offset - 1) & 0x80)) != 0) res = -res
    }
    res
  }


  override def commonPrefix(s: Sequence, skip: Short = 0): Short = {
    val that = s.asInstanceOf[TwoBitPrefixSeq5]
    val min = scala.math.min(this.length, that.length)
    var res: Int = min
    var i = skip / 4
    val end = if (min % 4 == 0) min / 4 else min / 4 + 1
    while (i < end && (this.seq(i + this.offset) == that.seq(i + that.offset))) i += 1
    if (i < end) {
      res = i * 4
      while (res < min && this.codePointAt(res) == that.codePointAt(res)) res += 1
    }
    res.toShort
  }
}
