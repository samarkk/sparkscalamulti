import sbt.Keys.libraryDependencies

name := "SparkMulti"

ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    sql,
    s3ops,
    streaming
  )

lazy val core = project
  .in(file("core"))
  .settings(
    name := "sparkcore",
    libraryDependencies ++= commonDependencies
  )
  .disablePlugins(AssemblyPlugin)

lazy val sql = project
  .in(file("sql"))
  .settings(
    name := "sparksql",
    libraryDependencies ++= commonDependencies ++ mysqldeps
  )
  .disablePlugins(AssemblyPlugin)

lazy val s3ops = project
  .in(file("s3ops"))
  .settings(
    name := "sparks3ops",
    libraryDependencies ++= commonDependencies ++ s3Dependencies
  )
  .disablePlugins(AssemblyPlugin)

lazy val streaming = project
  .in(file("streaming"))
  .settings(
    name := "sparkstreamingsregandsql",
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ kafkadeps ++ mysqldeps
  )

lazy val dependencies = {
  new {
    val sparkV = "3.3.0"
    val kafkaV = "3.0.1"
    val sparkcore = "org.apache.spark" %% "spark-core" % sparkV % Provided
    val sparksql = "org.apache.spark" %% "spark-sql" % sparkV % Provided

    val hadoopaws = "org.apache.hadoop" % "hadoop-aws" % "3.3.2"
    val awssdkbundle = "com.amazonaws" % "aws-java-sdk" % "1.11.1026"

    val mysqljava = "mysql" % "mysql-connector-java" % "8.0.29"
    val sparkstreaming =
      "org.apache.spark" %% "spark-streaming" % sparkV % Provided
    val kafkastreaming =
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV
    val kafkasql = "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0"
    val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaV
  }
}

lazy val commonDependencies = Seq(dependencies.sparkcore, dependencies.sparksql)
lazy val s3Dependencies = Seq(dependencies.hadoopaws, dependencies.awssdkbundle)
lazy val mysqldeps = Seq(dependencies.mysqljava)
lazy val kafkadeps = Seq(
  dependencies.sparkstreaming,
  dependencies.kafkastreaming,
  dependencies.kafkasql,
  dependencies.kafkaClients
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := name.value + ".jar",
  assembly / assemblyMergeStrategy := {
    case "reference.conf" => MergeStrategy.concat
    // doing this on windows the match will happen only with \\ and not with /
    // tried multiple times with / but every time this gy below was discarded
    case "META-INF\\services\\org.apache.spark.sql.sources.DataSourceRegister" =>
      MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => {
//      println(s"MergeStrattegy first applied to $x")
      MergeStrategy.first
    }
  }
)
