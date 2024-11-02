package com.etl_audit

import java.sql.Connection
import java.time.LocalDateTime

case class ETLNotificationLog(
  notificationId: Int,
  jobId: Option[String],
  batchId: Option[String],
  stepId: Option[Int],
  notificationType: String,
  recipient: String,
  sentAt: LocalDateTime,
  message: String
  )

object ETLNotificationLog {
  def insert(notificationLog: ETLNotificationLog)(implicit conn: Connection): Int = {
    val sql = """
      INSERT INTO etl_notification_log (
        job_id, batch_id, step_id, notification_type, recipient, sent_at, message
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    val statement = conn.prepareStatement(sql)
    statement.setString(1, notificationLog.jobId.orNull)
    statement.setString(2, notificationLog.batchId.orNull)
    statement.setInt(3, notificationLog.stepId.getOrElse(0))
    statement.setString(4, notificationLog.notificationType)
    statement.setString(5, notificationLog.recipient)
    statement.setTimestamp(6, java.sql.Timestamp.valueOf(notificationLog.sentAt))
    statement.setString(7, notificationLog.message)
    statement.executeUpdate()
  }
}
