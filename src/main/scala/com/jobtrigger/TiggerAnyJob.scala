package com.jobtrigger

import com.dbutils.DatabaseConfig
import com.etl_audit.ETLJobAudit
import java.util.UUID
import java.time.LocalDateTime

object TiggerAnyJob extends App {

  implicit val conn = DatabaseConfig.getConnection

  // Determine the job arguments, using defaults if none are provided
  private val jobArgs: Array[String] = if (args.isEmpty) {
    Array(
      "C:\\Users\\pedam\\PycharmProjects\\pythonProject\\configs.properties",
      "framework-variables",
      "SampleApp" // Default job name here
    )
  } else {
    args
  }

  // Get the job name from arguments, defaulting to "SampleApp"
  val jobName = jobArgs.lastOption.getOrElse("SampleApp").toLowerCase
  val jobApp = JobRegistry.getJobApp(jobName)

  // Generate a unique job ID
  val jobId = s"job_${UUID.randomUUID()}"
  val submittedBy = System.getProperty("user.name")
  val submittedAt = getCurrentTimeWithMilliseconds
  var startedAt: Option[LocalDateTime] = None
  val status = "Submitted"
  var completedAt: Option[LocalDateTime] = None

  // Create and insert the initial job audit record
  val jobAudit = createJobAudit(jobId, jobName, submittedBy, submittedAt)

  // Record the start time and execute the job
  startedAt = Some(getCurrentTimeWithMilliseconds)
  runJob(jobApp)
  Thread.sleep(150) // Simulating some processing time
  completedAt = Some(getCurrentTimeWithMilliseconds)

  // Update the job audit record
  updateJobAudit(jobAudit.jobId, startedAt.get, completedAt.get)

  // Close the database connection
  conn.close()
  println("Job audit record with completion time updated successfully.")

  // Run the specified job application
  def runJob(jobApp: AppTrigger): Unit = {
    jobApp.run(jobArgs.init) // Run the job with all arguments except the job name
  }

  // Create a job audit record
  private def createJobAudit(jobId: String, jobName: String, submittedBy: String, submittedAt: LocalDateTime): ETLJobAudit = {
    val jobAudit = ETLJobAudit(
      jobId = jobId,
      jobName = jobName,
      submittedBy = submittedBy,
      submittedAt = submittedAt,
      startedAt = None,
      completedAt = None,
      status = status,
      errorMessage = None
    )
    ETLJobAudit.insert(jobAudit) // Insert the created job audit record
    jobAudit // Return the created job audit
  }

  // Update the existing job audit record in the database
  private def updateJobAudit(jobId: String, startedAt: LocalDateTime, completedAt: LocalDateTime): Unit = {
    val updatedJobAudit = ETLJobAudit(
      jobId = jobId,
      jobName = jobId.split("_")(1), // Assuming you want to keep the job name the same; adjust if needed
      submittedBy = System.getProperty("user.name"),
      submittedAt = getCurrentTimeWithMilliseconds, // This might need to be preserved, depending on your requirements
      startedAt = Some(startedAt),
      completedAt = Some(completedAt),
      status = "Completed",
      errorMessage = None
    )
    ETLJobAudit.update(updatedJobAudit) // This should call an update method in your ETLJobAudit
  }

  // Get the current time with milliseconds
  private def getCurrentTimeWithMilliseconds: LocalDateTime = {
    LocalDateTime.now()
  }
}
