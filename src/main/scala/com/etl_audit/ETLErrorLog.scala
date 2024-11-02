package com.etl_audit

import java.sql.Connection
import java.time.LocalDateTime

case class ETLErrorLog(
  errorId: Int,
  jobId: Option[String],
  batchId: Option[String],
  stepId: Option[Int],
  errorTimestamp: LocalDateTime,
  errorMessage: String,
  errorDetails: Option[String] = None
  )

object ETLErrorLog {
  def insert(errorLog: ETLErrorLog)(implicit conn: Connection): Int = {
    val sql = """
      INSERT INTO etl_error_log (
        job_id, batch_id, step_id, error_timestamp, error_message, error_details
      ) VALUES (?, ?, ?, ?, ?, ?)
    """
    val statement = conn.prepareStatement(sql)
    statement.setString(1, errorLog.jobId.orNull)
    statement.setString(2, errorLog.batchId.orNull)
    statement.setInt(3, errorLog.stepId.getOrElse(0))
    statement.setTimestamp(4, java.sql.Timestamp.valueOf(errorLog.errorTimestamp))
    statement.setString(5, errorLog.errorMessage)
    statement.setString(6, errorLog.errorDetails.orNull)
    statement.executeUpdate()
  }
}
