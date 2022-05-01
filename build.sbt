ThisBuild / version := "0.1.0-SNAPSHOT"

val sparkVersion = "3.1.3"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "MaNa_top500songs"
  )
libraryDependencies ++= Seq(
  // Apache Commons
  "commons-io" % "commons-io" % "2.6",

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)


