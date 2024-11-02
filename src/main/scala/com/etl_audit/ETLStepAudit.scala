package com.etl_audit

import java.sql.Connection
import java.time.LocalDateTime

case class ETLStepAudit(
  stepId: Int,
  batchId: String,
  stepName: String,
  startedAt: LocalDateTime,
  completedAt: Option[LocalDateTime] = None,
  status: String,
  rowCount: Option[Int] = None,
  errorMessage: Option[String] = None
  )

object ETLStepAudit {
  def insert(stepAudit: ETLStepAudit)(implicit conn: Connection): Int = {
    val sql = """
      INSERT INTO etl_step_audit (
        batch_id, step_name, started_at, completed_at, status, row_count, error_message
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    val statement = conn.prepareStatement(sql)
    statement.setString(1, stepAudit.batchId)
    statement.setString(2, stepAudit.stepName)
    statement.setTimestamp(3, java.sql.Timestamp.valueOf(stepAudit.startedAt))
    statement.setTimestamp(4, stepAudit.completedAt.map(java.sql.Timestamp.valueOf).orNull)
    statement.setString(5, stepAudit.status)
    statement.setInt(6, stepAudit.rowCount.getOrElse(0))
    statement.setString(7, stepAudit.errorMessage.orNull)
    statement.executeUpdate()
  }
}
