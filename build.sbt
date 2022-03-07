ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "P3",
    run / fork := true,
    run / connectInput := true,
//This gives a new name for the jar file so we can find it easier in the target/scala folder
    assembly / assemblyJarName := "P3.jar",
//When using sbt-assembly we can encounter errors caused by the default deduplicate merge strategy.
//In most cases this is caused by files in the META-INF directory.
//This code resolves merge issues by choosing the appropriate merge strategy for the paths
//that are causing errors.
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x                             => MergeStrategy.first
    }
  )

lazy val excludeJpountz =
  ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-parser-combinators
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams-scala
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.0.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.2",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.12.2"
  )
}

// For Name Generation/Fabrication
resolvers += "Fabricator" at "https://dl.bintray.com/biercoff/Fabricator"
libraryDependencies += "com.github.azakordonets" % "fabricator_2.12" % "2.1.5"

/*
//This gives a new name for the jar file so we can find it easier in the target/scala folder
assemblyJarName in assembly := "P3.jar"
//When using sbt-assembly we can encounter errors caused by the default deduplicate merge strategy.
//In most cases this is caused by files in the META-INF directory.
//This code resolves merge issues by choosing the appropriate merge strategy for the paths
//that are causing errors.
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
 */
