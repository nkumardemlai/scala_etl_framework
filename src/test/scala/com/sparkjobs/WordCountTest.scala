package com.sparkjobs

/**
 * A simple test for everyone's favourite wordcount example.
 */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite


class WordCountTest extends AnyFunSuite with SharedSparkContext {
  test("word count with Stop Words Removed") {
    val linesRDD = sc.parallelize(Seq(
      "How happy was the panda? You ask.",
      "Panda is the most happy panda in all the#!?ing land!"
    ))

    val stopWords: Set[String] = Set("a", "the", "in", "was", "there", "she", "he")
    val splitTokens: Array[Char] = "#%?!. ".toCharArray

    val wordCounts = WordCount.withStopWordsFiltered(
      linesRDD, splitTokens, stopWords
    )
    val wordCountsAsMap = wordCounts.collectAsMap()

    assert(!wordCountsAsMap.contains("the"))          // Check "the" is removed
    assert(!wordCountsAsMap.contains("?"))             // Ensure special chars are removed
    assert(!wordCountsAsMap.contains("#!?ing"))        // Ensure combined chars are removed
    assert(wordCountsAsMap.contains("ing"))            // "ing" should be counted correctly
    assert(wordCountsAsMap.getOrElse("panda", 0) == 3) // "panda" should have a count of 3
  }


}
