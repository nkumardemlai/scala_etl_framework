import sbt.*
import sbt.Keys.*

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.13"
    )),
    name := "sparkProject",
    version := "0.0.1",

    sparkVersion := "3.3.0",

    // Use Java 11
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    Test / parallelExecution := false,
    fork / run := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion.value,
      "org.apache.spark" %% "spark-sql" % sparkVersion.value,
      "org.apache.spark" %% "spark-core" % sparkVersion.value,
      "mysql" % "mysql-connector-java" % "8.0.33",
      "org.scalatest" %% "scalatest" % "3.2.2" % Test,
      "org.scalacheck" %% "scalacheck" % "1.15.2" % Test,
      "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_1.3.0" % Test
    )
    ,

    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated,

    resolvers ++= Resolver.sonatypeOssRepos("snapshots"),

    pomIncludeRepository := { _ => false },

    // Publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
