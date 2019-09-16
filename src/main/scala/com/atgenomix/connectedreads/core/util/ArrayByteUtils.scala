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

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer

object ArrayByteUtils {
  /**
    * Encode string to 2-bit char array with starting index
    *
    * @param s INPUT the input string
    * @param start INPUT the start position of s
    * @param i INPUT the starting index of s
    * @param b OUTPUT the encoded bytearray
    * @return
    */
  def encode5plus(s: String, start: Short, i: Int = 0, read_byte_length: Int, b: Array[Byte]): Unit = {
    val offset = read_byte_length * i
    var cnt = (s.length - start) / 4
    
    //add suffix
    var x = 0
    while (x < cnt) {
      val j = start + x * 4
      val v = char4ToByte(s(j)) << 6 | char4ToByte(s(j + 1)) << 4 | char4ToByte(s(j + 2)) << 2 | char4ToByte(s(j + 3))
      b(offset + x) = v.toByte
      x = x + 1
    }
    
    var bound = start + x * 4
    var tail = 0
    var z = 0
    while (bound < s.length) {
      val shift = (z & 0x03: @switch) match {
        case 0 => char4ToByte(s(bound)) << 6
        case 1 => char4ToByte(s(bound)) << 4
        case 2 => char4ToByte(s(bound)) << 2
        case 3 => char4ToByte(s(bound))
      }
      tail = tail | shift
      bound = bound + 1
      z = z + 1
    }
    b(offset+ x) = tail.toByte
    
    //add prefix
    cnt = start / 4
    val offset_prefix = x + 1 //the offset for prefix
    x = 0
    while (x < cnt) {
      val j = start - 1 - (x * 4)
      val v = char4ToByte(s(j)) << 6 | char4ToByte(s(j - 1)) << 4 | char4ToByte(s(j - 2)) << 2 | char4ToByte(s(j - 3))
      b(offset + offset_prefix + x) = v.toByte
      x = x + 1
    }
    
    bound = start - 1 - (x * 4)
    tail = 0
    z = 0
    while (bound >= 0) {
      val shift = (z & 0x03: @switch) match {
        case 0 => char4ToByte(s(bound)) << 6
        case 1 => char4ToByte(s(bound)) << 4
        case 2 => char4ToByte(s(bound)) << 2
        case 3 => char4ToByte(s(bound))
      }
      tail = tail | shift
      bound = bound - 1
      z = z + 1
    }
    b(offset + offset_prefix + x) = tail.toByte
  }
  
  /**
    * Encode string to 2-bit char array with starting index
    *
    * @param s the input string
    * @param i the starting index of s
    * @return (encoded byte array, length with $ terminating char)
    */
  def encode2bit(s: String, i: Int = 0): (Array[Byte], Short) = {
    val slen = s.length - i
    val alen = slen / 4 + 1
    val array = new Array[Byte](alen)
    
    var x = 0
    while (x < alen - 1) {
      val j = x * 4 + i
      val b = char4ToByte(s(j)) << 6 | char4ToByte(s(j + 1)) << 4 | char4ToByte(s(j + 2)) << 2 | char4ToByte(s(j + 3))
      array(x) = b.toByte
      x = x + 1
    }
    
    var bound = x * 4 + i
    var tail = 0
    var z = 0
    while (bound < s.length) {
      val shift = (z & 0x03: @switch) match {
        case 0 => char4ToByte(s(bound)) << 6
        case 1 => char4ToByte(s(bound)) << 4
        case 2 => char4ToByte(s(bound)) << 2
        case 3 => char4ToByte(s(bound))
      }
      tail = tail | shift
      bound = bound + 1
      z = z + 1
    }
    array(alen - 1) = tail.toByte
    (array, (slen + 1).toShort)
  }
  
  def encode(s: String, i: Int = 0): (Array[Byte], Short) = {
    val slen = s.length - i
    val alen = slen / 4 + 1
    val array = new Array[Byte](alen)
    
    var x = 0
    while (x < alen - 1) {
      val j = x * 4 + i
      val b = char4ToByte(s(j)) << 6 | char4ToByte(s(j + 1)) << 4 | char4ToByte(s(j + 2)) << 2 | char4ToByte(s(j + 3))
      array(x) = b.toByte
      x = x + 1
    }
    
    var bound = x * 4 + i
    var tail = 0
    var z = 0
    while (bound < s.length) {
      val shift = (z & 0x03: @switch) match {
        case 0 => char4ToByte(s(bound)) << 6
        case 1 => char4ToByte(s(bound)) << 4
        case 2 => char4ToByte(s(bound)) << 2
        case 3 => char4ToByte(s(bound))
      }
      tail = tail | shift
      bound = bound + 1
      z = z + 1
    }
    array(alen - 1) = tail.toByte
    (array, (slen + 1).toShort)
  }
  
  // convert the given string into Array[Byte]; and each byte contains two chars.
  // for odd-characters cases, the tail naturally implies the `$`; and even cases
  // will add one more byte with zero value to represents the dollar sign.
  // So in our usage scenario, the encoded array must end up with a `$`.
  def encode4bit(s: String): Array[Byte] = {
    val array = new Array[Byte]((s.length / 2) + 1)
    for (i <- 0 until s.length by 2) {
      array(i / 2) =
        if ((i + 2) <= s.length)
          (char2ToByte(s(i)) << 4 | char2ToByte(s(i + 1))).toByte
        else
        // last chunk in odd case, the left part always has value and end with "0000" naturally, i.e., `$`
          (char2ToByte(s(i)) << 4).toByte
    }
    array
  }
  
  /**
    * Encode string to 4-bit char array with starting index
    *
    * @param s the input string
    * @param i the starting index of s
    * @return
    */
  def encode4bit(s: String, i: Int): Array[Byte] = {
    val array = new Array[Byte](((s.length - i) / 2) + 1)
    for (j <- i until s.length by 2) {
      array((j - i) / 2) =
        if ((j + 2) <= s.length)
          (char2ToByte(s(j)) << 4 | char2ToByte(s(j + 1))).toByte
        else
        // last chunk in odd case, the left part always has value and end with "0000" naturally, i.e., `$`
          (char2ToByte(s(j)) << 4).toByte
    }
    array
  }
  
  // The goal is to compress to chars into a byte, i.e., each char takes 4-bits space.
  // Therefore, we assign the corresponding values for `A C G T N $`.
  // e.g., A => 0001, AA => 0001 0001, TA => 0100 0001
  private def char2ToByte(c: Char@switch): Byte = c match {
    case '$' => 0
    case 'A' => 1
    case 'C' => 2
    case 'G' => 3
    case 'N' => 4
    case 'T' => 5
    case _ => throw new IllegalArgumentException
  }
  
  // 2-bit encoding; 1 byte = 8 bits = 4 chars (1 char = 2 bits [A, C, G, T])
  private[this] def char4ToByte(c: Char@switch): Byte = c match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => throw new IllegalArgumentException
  }

  // since we encapsulate two chars into a byte that we need to identify the `$` in the last element
  def len(arr: Array[Byte]): Int = {
    val tail = if (((arr.last >> 4) & 0x0F) == 0) 1 else 2
    (arr.length - 1) * 2 + tail
  }

  def charAt(arr: Array[Byte], pos: Int): Char = {
    // if pos is divisible by 2 take the LEFT part else get the RIGHT part
    if (pos % 2 == 0) int2ToChar((arr(pos / 2) & 0xf0) >> 4) else int2ToChar(arr(pos / 2) & 0x0f)
  }

  def byteAt(arr: Array[Byte], pos: Int): Int = {
    // if pos is divisible by 2 take the LEFT part else get the RIGHT part
    if (pos % 2 == 0) (arr(pos / 2) & 0xf0) >> 4 else arr(pos / 2) & 0x0f
  }

  def hash(arr: Array[Byte]): Long = {
    var i: Long = 1125899906842597L // mixingPrime
    for (b <- arr) {
      i = 31 * i + ((b >> 4) & 0x0F) // higher bits always have value in our data structure
      if ((b & 0x0F) > 0) i = 31 * i + (b & 0x0F) // if the value of lower bits equals to zero represents there is no char
    }
    i
  }
  
  def hash2bit(arr: Array[Byte]): Long = {
    var h: Long = 1125899906842597L // mixingPrime
    for (a <- arr) {
      h = if ((a & 0xF0) > 0) 31 * h + ((a >> 4) & 0x0F) else 31 * h + 17
      h = if ((a & 0x0F) > 0) 31 * h + (a & 0x0F) else 31 * h + 17
    }
    h
  }

  // this method is mainly used in the interpretation of part of the sub-array-bytes,
  // so the `$` will not be added automatically
  def subdecode(arr: Array[Byte], start: Int, end: Int): String = {
    //      println("\n\n\n\n" + subarraybyte(start, end).deep.mkString("\n") + "\n\n\n\n")
    until(subarraybyte(arr, start, end), end - start)
  }
  
  def subdecode2bit(arr: Array[Byte], start: Int, end: Int): String = {
    val s = new StringBuilder
    var i = start
    while (i < end) {
      val b = i % 4 match {
        case 0 => arr(i / 4).toInt >> 6
        case 1 => arr(i / 4).toInt >> 4
        case 2 => arr(i / 4).toInt >> 2
        case 3 => arr(i / 4).toInt
      }
      s += int4ToChar(b & 0x000000FF)
      i += 1
    }
    s.toString
  }

  // selects first n-1 elements
  def until(arr: Array[Byte], n: Int): String = {
    val buf = new StringBuilder
    var i = 0
    while (i < n / 2) {
      buf += (int2ToChar((arr(i) & 0xf0) >> 4), int2ToChar(arr(i) & 0x0f))
      i = i + 1
    }
    if (n % 2 != 0) buf += int2ToChar((arr(n / 2) & 0xf0) >> 4)
    buf.toString
  }

  def subarraybyte(arr: Array[Byte], start: Int, end: Int): Array[Byte] = {
    require(end > start, s"end '$end' must be greater than start '$start' ==> ${decode(arr)}")
    (start, end) match {
      case (s, e) if s % 2 == 0 && e % 2 == 0 => arr.slice(s / 2, e / 2) // (0,8) => AA TA GG GT, (2,4) => TA
      case (s, e) if s % 2 == 0 && e % 2 != 0 => // (0,7) => AA TA GG G
        val b = arr.slice(s / 2, e / 2 + 1)
        b((e - s) / 2) = (b((e - s) / 2) & 0xF0).toByte // keep the higher-bits
        b
      case (s, e) if s % 2 != 0 && e % 2 == 0 => // odd_start_even_end, e.g., (1,6) => AT AG G
        val buf = new Array[Byte]((e - s) / 2 + 1)
        for (i <- s until e - 1 by 2)
          buf((i - s) / 2) = ((lowerBitsAt(arr, i) << 4) | (upperBitsAt(arr, i + 1) >> 4)).toByte
        buf(buf.length - 1) = (lowerBitsAt(arr, e - 1) << 4).toByte // last element
        buf
      case (s, e) if s % 2 != 0 && e % 2 != 0 => // odd_start_odd_end, e.g., (1,5) => ATAG
        val b = arr.slice(s / 2, e / 2 + 1)
        var x = b(0) & 0x0F
        // lower-bits of the first element
        val buf = new Array[Byte](b.length - 1)
        for (i <- 1 until b.length) {
          buf(i - 1) = ((x << 4) | (b(i) >> 4) & 0x0F).toByte
          x = b(i) & 0x0F
        }
        buf
      case _ => throw new IllegalArgumentException
    }
  }
  
  def sub2BitByteArray(arr: Array[Byte], start: Int, end: Int): Array[Byte] = {
    require(end > start)
    (start, end) match {
      case (s, e) if s % 4 == 0 && e % 4 == 0 =>
        arr.slice(s / 4, e / 4)
      case (s, e) if s % 4 == 0 && e % 4 != 0 =>
        arr.slice(s / 4, e / 4 + 1)
      case (s, e) if s % 4 != 0 =>
        val sub = if ((e - s) % 4 > 0) new Array[Byte]((e - s) / 4 + 1) else new Array[Byte]((e - s) / 4)
        var i = s
        var j = 0
        while (i < e) {
          sub(j) = if (i + 4 <= e) byteAt2Bit(arr, i, 4) else byteAt2Bit(arr, i, e)
          i += 4
          j += 1
        }
        sub
    }
  }
  
  def append2BitByteArray(a: ArrayBuffer[Byte], al: Int, b: Array[Byte], bs: Int, bl: Int): ArrayBuffer[Byte] = {
    al % 4 match {
      case 0 =>
        a ++= b
      case _ =>
        var l = al
        for (i <- bs until bl) {
          if (l % 4 == 0) a += 0.toByte
          val w = a(l / 4).toInt
          val v = b(i / 4).toInt
          (l % 4, i % 4) match {
            case (0, 0) => a(l / 4) = ((w | v) & 0x000000C0).toByte
            case (0, 1) => a(l / 4) = ((w | (v << 2)) & 0x000000C0).toByte
            case (0, 2) => a(l / 4) = ((w | (v << 4)) & 0x000000C0).toByte
            case (0, 3) => a(l / 4) = ((w | (v << 6)) & 0x000000C0).toByte
            case (1, 0) => a(l / 4) = ((w | ((v >> 2) & 0x00000030)) & 0x000000F0).toByte
            case (1, 1) => a(l / 4) = ((w | v) & 0x000000F0).toByte
            case (1, 2) => a(l / 4) = ((w | ((v << 2) & 0x00000030)) & 0x000000F0).toByte
            case (1, 3) => a(l / 4) = ((w | ((v << 4) & 0x00000030)) & 0x000000F0).toByte
            case (2, 0) => a(l / 4) = ((w | ((v >> 4) & 0x0000000C)) & 0x000000FC).toByte
            case (2, 1) => a(l / 4) = ((w | ((v >> 2) & 0x0000000C)) & 0x000000FC).toByte
            case (2, 2) => a(l / 4) = ((w | v) & 0x000000FC).toByte
            case (2, 3) => a(l / 4) = ((w | ((v << 2) & 0x0000000C)) & 0x000000FC).toByte
            case (3, 0) => a(l / 4) = ((w | ((v >> 6) & 0x00000003)) & 0x000000FF).toByte
            case (3, 1) => a(l / 4) = ((w | ((v >> 4) & 0x00000003)) & 0x000000FF).toByte
            case (3, 2) => a(l / 4) = ((w | ((v >> 2) & 0x00000003)) & 0x000000FF).toByte
            case (3, 3) => a(l / 4) = ((w | v) & 0x000000FF).toByte
          }
          l += 1
        }
        a
    }
  }
  
  def byteAt2Bit(arr: Array[Byte], start: Int, end: Int): Byte = {
    require(end > start && end - start <= 4)
    var b1 = arr(start / 4).toInt
    start % 4 match {
      case 0 =>
      case 1 => b1 = b1 << 2
      case 2 => b1 = b1 << 4
      case 3 => b1 = b1 << 6
    }
    var b2 = arr(end / 4).toInt
    end % 4 match {
      case 0 =>
      case 1 => b2 = b2 >> 6
      case 2 => b2 = b2 >> 4
      case 3 => b2 = b2 >> 2
    }
    ((b1 & 0x000000FF) | b2).toByte
  }

  def upperBitsAt(arr: Array[Byte], pos: Int): Int = arr(pos / 2) & 0xF0

  def lowerBitsAt(arr: Array[Byte], pos: Int): Int = arr(pos / 2) & 0x0F

  def decode(arr: Array[Byte]): String = {
    /*
        A byte contains two chars, e.g., 0001 0010

          0001 0010
        & 1111 0000 (0xf0)
          ---------
          0001 0000 >> 4 => 0001 => 'A'

          0001 0010
        & 0000 1111 (0x0f)
          ---------
          0000 0010 => 'C'

        the first n-1 items can be decoded directly, the last item has `$` on the upper 4 bits or not
     */
    val buf = new StringBuilder
    for (i <- 0 until arr.length - 1) {
      buf += (int2ToChar((arr(i) & 0xf0) >> 4), int2ToChar(arr(i) & 0x0f)) // (left 4 bits, right 4 bits)
    }

    val tail = int2ToChar((arr(arr.length - 1) & 0xf0) >> 4)
    buf += tail
    if (tail != '$') buf += '$'

    buf.toString
  }

  private[this] def int2ToChar(b: Int@switch): Char = b match {
    case 0 => '$'
    case 1 => 'A'
    case 2 => 'C'
    case 3 => 'G'
    case 4 => 'N'
    case 5 => 'T'
    case _ => throw new IllegalArgumentException(s"Unknown option: '$b'")
  }
  
  private[this] def int4ToChar(i: Int@switch): Char = i match {
    case 0 => 'A'
    case 1 => 'C'
    case 2 => 'G'
    case 3 => 'T'
    case _ => throw new IllegalArgumentException
  }
}
