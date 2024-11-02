package com.sparkjobs.pipelines

import com.utils.ConfigParser

object sample_obj {

 // def main(args: Array[String]): Unit = {

    val filepath = "C:\\Users\\pedam\\PycharmProjects\\pythonProject\\configs.properties"
    val parser = new ConfigParser(filepath)
    parser.parse()
    val sectionName = "framework-variables"
    // Example: Access a particular property
    val hadoopPath = parser.getProperty(sectionName, "hadoop_java_path")

    println(s"Hadoop Java Path: " + hadoopPath)

//  }

}
