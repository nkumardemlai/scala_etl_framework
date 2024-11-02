package com.etl_audit

import java.sql.Connection
import java.time.LocalDateTime

case class ETLJobAudit(
                        jobId: String,
                        jobName: String,
                        submittedBy: String,
                        submittedAt: LocalDateTime,
                        startedAt: Option[LocalDateTime] = None,
                        completedAt: Option[LocalDateTime] = None,
                        status: String,
                        errorMessage: Option[String] = None
                      )

object ETLJobAudit {
  def insert(jobAudit: ETLJobAudit)(implicit conn: Connection): Int = {
    val sql = """
      INSERT INTO etl_job_audit (
        job_id, job_name, submitted_by, submitted_at, started_at, completed_at, status, error_message
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    val statement = conn.prepareStatement(sql)
    statement.setString(1, jobAudit.jobId)
    statement.setString(2, jobAudit.jobName)
    statement.setString(3, jobAudit.submittedBy)
    statement.setTimestamp(4, java.sql.Timestamp.valueOf(jobAudit.submittedAt))
    statement.setTimestamp(5, jobAudit.startedAt.map(java.sql.Timestamp.valueOf).orNull)
    statement.setTimestamp(6, jobAudit.completedAt.map(java.sql.Timestamp.valueOf).orNull)
    statement.setString(7, jobAudit.status)
    statement.setString(8, jobAudit.errorMessage.orNull)
    statement.executeUpdate()
  }

  def update(jobAudit: ETLJobAudit)(implicit conn: Connection): Unit = {
    val sql = "UPDATE etl_job_audit SET started_at = ?, completed_at = ?, status = ? WHERE job_id = ?"
    val preparedStatement = conn.prepareStatement(sql)
    preparedStatement.setTimestamp(1, java.sql.Timestamp.valueOf(jobAudit.startedAt.get))
    preparedStatement.setTimestamp(2, java.sql.Timestamp.valueOf(jobAudit.completedAt.get))
    preparedStatement.setString(3, jobAudit.status)
    preparedStatement.setString(4, jobAudit.jobId)
    preparedStatement.executeUpdate()
  }

}
