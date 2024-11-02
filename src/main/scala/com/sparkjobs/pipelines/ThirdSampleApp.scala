package com.sparkjobs.pipelines

import com.jobtrigger.AppTrigger
import com.sparkjobs.inits.SparkAppInitializer
import org.apache.spark.sql.DataFrame

object ThirdSampleApp extends App with AppTrigger{
  override def run(arguments: Array[String]): Unit = {
    main(arguments)
  }

  val (arg1, arg2) = if (args.isEmpty) {
    ("C:\\Users\\pedam\\PycharmProjects\\pythonProject\\configs11.properties", "framework-variables11")
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
  spark.sql("select * from bisket").show(30,false)
  println(init.ANSI_RESET)

  // Stop the SparkSession
  spark.stop()
}
