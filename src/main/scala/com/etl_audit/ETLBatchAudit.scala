package com.etl_audit

import java.sql.Connection
import java.time.LocalDateTime

case class ETLBatchAudit(
  batchId: String,
  jobId: String,
  batchName: String,
  startedAt: LocalDateTime,
  completedAt: Option[LocalDateTime] = None,
  status: String,
  errorMessage: Option[String] = None
  )

object ETLBatchAudit {
  def insert(batchAudit: ETLBatchAudit)(implicit conn: Connection): Int = {
    val sql = """
      INSERT INTO etl_batch_audit (
        batch_id, job_id, batch_name, started_at, completed_at, status, error_message
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    val statement = conn.prepareStatement(sql)
    statement.setString(1, batchAudit.batchId)
    statement.setString(2, batchAudit.jobId)
    statement.setString(3, batchAudit.batchName)
    statement.setTimestamp(4, java.sql.Timestamp.valueOf(batchAudit.startedAt))
    statement.setTimestamp(5, batchAudit.completedAt.map(java.sql.Timestamp.valueOf).orNull)
    statement.setString(6, batchAudit.status)
    statement.setString(7, batchAudit.errorMessage.orNull)
    statement.executeUpdate()
  }
}
