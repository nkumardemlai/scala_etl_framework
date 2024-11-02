package com.utils

import scala.collection.mutable
import scala.io.Source
import scala.util.Try

class ConfigParser(filePath: String) {

  // Changed to Map[String, String]
  private val configMap = mutable.Map[String, Map[String, String]]()

  def parse(): Unit = {
    val source = Try(Source.fromFile(filePath))
    var currentSection: Option[String] = None
    var properties = mutable.Map[String, String]()

    try {
      source.get.getLines().foreach { line =>
        val trimmedLine = line.trim
        if (trimmedLine.isEmpty || trimmedLine.startsWith("#")) {
          // Skip empty lines or comments
        } else if (trimmedLine.startsWith("[") && trimmedLine.endsWith("]")) {
          // New section // No need to change to mutable.Map
          currentSection.foreach { section =>
            configMap(section) = properties.toMap
          }
          currentSection = Some(trimmedLine.substring(1, trimmedLine.length - 1).trim)
          properties = mutable.Map[String, String]()
        } else if (currentSection.isDefined && trimmedLine.contains("=")) {
          // Key-value pair
          val Array(key, value) = trimmedLine.split("=", 2).map(_.trim)
          properties(key) = value
        }
      }
      // Add the last section
      currentSection.foreach { section =>
        configMap(section) = properties.toMap
      }
    } finally {
      source.get.close()
    }
  }

  def getProperty(section: String, key: String): String = {
    configMap.get(section).flatMap(_.get(key)) match {
      case Some(value) => value
      case None =>
        println(s"$key not found in section $section")
        ""
    }
  }
}
