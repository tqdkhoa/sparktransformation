name := "sparktransformation"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "com.azure" % "azure-storage-blob" % "12.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5"
)