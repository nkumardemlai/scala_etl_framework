package com.sparkjobs.pipelines

import com.sparkjobs.inits.SparkAppInitializer
import org.apache.spark.sql.DataFrame

object JsonReaderApp extends App {

  val (arg1, arg2) = if (args.isEmpty) {
    // Read the JSON file into a DataFrame
    // sbt "runMain com.example.sparkproject.applications.JsonReaderApp
    // C:\Users\pedam\PycharmProjects\pythonProject\configs.properties framework-variables"
    ("C:\\Users\\pedam\\PycharmProjects\\pythonProject\\configs.properties", "framework-variables")
      } else {
    (args(0), args(1))
  }
  val init = new SparkAppInitializer(arg1, arg2)
  val spark = init.spark

  private val file_input = init.getProperty("file_json_input")

  private val srcData: DataFrame = spark.read.format("json")
    .option("inferSchema", "true")
    .option("multiline", "true")
    .option("mode", "DROPMALFORMED")
    .load(file_input)

  srcData.createOrReplaceTempView("bisket")

  println(init.ANSI_RED)
  spark.sql("select * from bisket").show(50,false)
  println(init.ANSI_RESET)

  // Stop the SparkSession
  spark.stop()
}

