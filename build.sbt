name := "InterviewTest"

version := "1.0"

scalaVersion := "2.11.8"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",

  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.3" % "test"
)