version := "1.0"

scalaVersion := "2.11.11"

fork := true

val versionSpark = "2.2.0"
val versionAvro  = "1.8.1"

libraryDependencies ++= Seq(
  "org.apache.spark"           %% "spark-core"                 % versionSpark,
  "org.apache.spark"           %% "spark-streaming"            % versionSpark,
  "org.apache.spark"           %% "spark-streaming-kafka-0-10" % versionSpark,
  "org.apache.spark"           %% "spark-sql-kafka-0-10"       % versionSpark,
  "org.apache.spark"           %% "spark-sql"                  % versionSpark,
  "org.apache.spark"           %% "spark-mllib"                % versionSpark,
  "org.apache.avro"            % "avro"                        % versionAvro,
  "com.sksamuel.avro4s"        %% "avro4s-core"                % versionAvro,
  "com.twitter"                %% "bijection-avro"             % "0.9.6",
  "com.typesafe"               % "config"                      % "1.3.1",
  "info.batey.kafka"           % "kafka-unit"                  % "0.6" % "test",
  "net.sf.jopt-simple"         % "jopt-simple"                 % "5.0.2",
  "com.typesafe.scala-logging" %% "scala-logging"              % "3.5.0",
  "io.spray"                   %% "spray-json"                 % "1.3.4"
)

scalacOptions += "-target:jvm-1.8"
