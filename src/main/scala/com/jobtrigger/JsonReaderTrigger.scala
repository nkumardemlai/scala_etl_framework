package com.jobtrigger

import com.dbutils.DatabaseConfig
import com.etl_audit.ETLJobAudit
import com.sparkjobs.pipelines.JsonReaderApp
import java.util.UUID
import java.time.LocalDateTime

object JsonReaderTrigger extends App {

  implicit val conn = DatabaseConfig.getConnection

  val arguments = Array(
    "C:\\Users\\pedam\\PycharmProjects\\pythonProject\\configs.properties",
    "framework-variables"
  )
  val jobAudit = ETLJobAudit(
    jobId = s"job_${UUID.randomUUID().toString}",
    jobName = "Daily Sales ETL",
    submittedBy = System.getProperty("user.name"),
    submittedAt = getCurrentTimeWithMilliseconds,
    startedAt = None,
    status = "Submitted",
    completedAt = None
  )
  try {
    val startedAt = getCurrentTimeWithMilliseconds

    JsonReaderApp.main(arguments)

    val completedAt = getCurrentTimeWithMilliseconds
    val updatedJobAudit = jobAudit.copy(completedAt = Some(completedAt), startedAt = Some(startedAt))
    ETLJobAudit.insert(updatedJobAudit)
    println("Job audit record with completion time inserted successfully.")
  } finally {
    conn.close()
  }


  private def getCurrentTimeWithMilliseconds: LocalDateTime = {
    val now = LocalDateTime.now()
    now
  }
}
