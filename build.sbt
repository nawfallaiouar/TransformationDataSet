name := "TransformationDataSet"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.4"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.10.0"
libraryDependencies += "com.crealytics" % "spark-excel_2.11" % "0.12.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.4"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.4"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.23"
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.7.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2",
  )
}