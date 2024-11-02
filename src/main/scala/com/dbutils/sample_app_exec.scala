package com.project.triggers

import com.dbutils.DatabaseConfig
import com.etl_audit.ETLJobAudit
import java.util.UUID
import java.time.LocalDateTime


object Main extends App {

  // Create a connection
  implicit val conn = DatabaseConfig.getConnection

  try {
    // Example: Insert a job audit record
    val jobAudit = ETLJobAudit(
      jobId = s"job_${UUID.randomUUID().toString}",
      jobName = "Daily Sales ETL",
      submittedBy = System.getProperty("user.name"),
      submittedAt = LocalDateTime.now(),
      status = "Submitted"
    )

    ETLJobAudit.insert(jobAudit)

    println("Job audit record inserted successfully.")
  } finally {
    // Ensure the connection is closed
    conn.close()
  }
}
