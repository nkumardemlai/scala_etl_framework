package com.sparkjobs.inits

import com.utils.ConfigParser
import org.apache.spark.sql.SparkSession

class SparkAppInitializer(filepath: String, sectionName: String) {

  // Initialize the ConfigParser
  private val parser = new ConfigParser(filepath)
  parser.parse()

  // Set system properties based on the config
  System.setProperty("hadoop.home.dir", parser.getProperty(sectionName, "hadoop_java_path"))
  System.setProperty("spark.sql.warehouse.dir", parser.getProperty(sectionName, "spark_java_warehouse"))

  val ANSI_RED = "\u001B[34m"
  val ANSI_RESET = "\u001B[0m"


  private val user = System.getProperty("user.name")
  println( ANSI_RED+s"Job submitted by user: $user" + ANSI_RESET)


  // Initialize SparkSession
  lazy val spark: SparkSession = {
    SparkSession.builder
      .appName("Simple Spark App")
      .master("local[*]")
      .getOrCreate()
  }

  // Set log level
  spark.sparkContext.setLogLevel("ERROR")

  // Method to get a property value
  def getProperty(key: String): String = parser.getProperty(sectionName, key)
}

