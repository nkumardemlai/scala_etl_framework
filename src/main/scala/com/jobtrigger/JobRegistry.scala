package com.jobtrigger

import com.sparkjobs.pipelines.{FirstSampleApp, SecondSampleApp, ThirdSampleApp}

object JobRegistry {
  def getJobApp(appName: String): AppTrigger = {
    appName.toLowerCase match {
      case "firstsampleapp" => FirstSampleApp
      case "secondsampleapp" => SecondSampleApp
      case "thirdsampleapp" => ThirdSampleApp
      case _ => throw new IllegalArgumentException(s"Unknown job application: $appName")
    }
  }
}
