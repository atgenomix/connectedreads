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

import org.apache.commons.math3.special.Gamma

object StatsUtils {
  type HgAccT = (Int, Int, Int, Int, Double)
  
  type FisherExactTest = (Double, Double, Double, Double)
  
  private def logBinomial(n: Int, k: Int): Double = {
    if (k == 0 || n == k) 0
    else Gamma.logGamma(n + 1) - Gamma.logGamma(k + 1) - Gamma.logGamma(n - k + 1)
  }
  
  /* hypergeometric distribution */
  private def hyperGeo(n11: Int, n1x: Int, nx1: Int, n: Int): Double = {
    Math.exp(logBinomial(n1x, n11) + logBinomial(n - n1x, nx1 - n11) - logBinomial(n, nx1))
  }
  
  /* incremental version of hypergeometric distribution */
  private def hyperGeoAcc(n11: Int, n1x: Int, nx1: Int, n: Int, aux: HgAccT): HgAccT = {
    if (n1x != 0 || nx1 != 0 || n != 0) {
      (n11, n1x, nx1, n, hyperGeo(n11, n1x, nx1, n))
    }
    else {
      var prob = aux._5
      
      /* then only n11 changed; the rest fixed */
      if (n11 % 11 != 0 && (n11 + aux._4 - aux._2 - aux._3) != 0) {
        if (n11 == aux._1 + 1) {
          /* incremental */
          prob *= ((aux._2 - aux._1) / n11 * (aux._3 - aux._1) / (n11 + aux._4 - aux._2 - aux._3))
        }
        else if (n11 == aux._1 - 1) {
          /* incremental */
          prob *= (aux._1 / (aux._2 - n11) * (aux._1 + aux._4 - aux._2 - aux._3) / (aux._3 - n11))
        }
        else {
          prob = hyperGeo(n11, aux._2, aux._3, aux._4)
        }
      }
      else {
        prob = hyperGeo(n11, aux._2, aux._3, aux._4)
      }
  
      (n11, aux._2, aux._3, aux._4, prob)
    }
  }
  
  def fisherExactTest(n11: Int, n12: Int, n21: Int, n22: Int): FisherExactTest = {
    val n = n11 + n12 + n21 + n22
    val n1x = n11 + n12
    val nx1 = n11 + n21
    val max = if (nx1 < n1x) nx1 else n1x /* max n11, for right tail */
    var min = n1x + nx1 - n /* min n11, for left tail */
    if (min < 0) min = 0
    
    if (min == max) {
      (1.0, 1.0, 1.0, 1.0)
    }
    else {
      var aux: HgAccT = (0, 0, 0, 0, 0.0)
      
      /* the probability of the current table */
      aux = hyperGeoAcc(n11, n1x, nx1, n, aux)
      val prob = aux._5
      
      /* left tail */
      aux = hyperGeoAcc(min, 0, 0, 0, aux)
      var left = 0.0
      var i = min + 1
      while (aux._5 < 0.99999999 * prob) { /* loop until underflow */
        left += aux._5
        aux = hyperGeoAcc(i, 0, 0, 0, aux)
        i += 1
      }
      i -= 1
      
      if (aux._5 < 1.00000001 * prob)
        left += aux._5
      else
        i -= 1
      
      /* right tail */
      aux = hyperGeoAcc(max, 0, 0, 0, aux)
      var right = 0.0
      var j = max - 1
      while (aux._5 < 0.99999999 * prob) { /* loop until underflow */
        right += aux._5
        aux = hyperGeoAcc(j, 0, 0, 0, aux)
        j -= 1
      }
      j += 1
      
      if (aux._5 < 1.00000001 * prob)
        right += aux._5
      else
        j += 1
      
      /* two-tail */
      var two = left + right
      if (two > 1.0) two = 1.0
      
      /* adjust left and right */
      if (Math.abs(i - n11) < Math.abs(j - n11))
        right = 1.0 - left + prob
      else
        left = 1.0 - right + prob
      
      (left, right, two, prob)
    }
  }
}
