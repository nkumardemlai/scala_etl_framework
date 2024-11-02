package com.sparkjobs.pipelines

import com.sparkjobs.inits.SparkAppInitializer

object WordCount extends App {

  if (args.length < 2) {
    println("Usage: WordCount <filepath> <sectionName>")
    sys.exit(1)
  }

  // Initialize the Init class
  val init = new SparkAppInitializer(args(0), args(1))
  val spark = init.spark

  private val sample_text_word_counts_path = init.getProperty("sample_text_word_counts_path")

   // Read input file into an RDD
  private val input = spark.sparkContext.textFile(sample_text_word_counts_path)

  // Perform the word count, filtering out numbers, symbols, etc. // Remove non-alphabetic characters
  // Filter out empty strings // Convert to lowercase for case-insensitive count
  // sbt "runMain com.example.sparkproject.applications.WordCount C:\Users\pedam\PycharmProjects\pythonProject\configs.properties framework-variables"

  private val counts = input.flatMap(line => line.split(" "))
    .map(_.replaceAll("[^A-Za-z]", ""))
    .filter(_.nonEmpty)
    .map(word => (word.toLowerCase, 1))
    .reduceByKey(_ + _)

  // Collect the word counts and sort by the word (dictionary order)
  private val sortedOutput = counts.sortBy(_._1).collect()

  // Display the sorted word counts
  sortedOutput.foreach { case (word, count) =>
    println(s"$word: $count")
  }

  // Stop the SparkContext
  spark.stop()
}
