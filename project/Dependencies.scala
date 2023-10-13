import sbt._

object Dependencies {

  val testDependencies: Seq[ModuleID] = Seq(
    "org.mockito"        %% "mockito-scala-scalatest"  % "1.17.12",
    "org.scalatestplus"  %% "scalatestplus-scalacheck" % "3.1.0.0-RC2",
    "org.pegdown"        % "pegdown"                   % "1.6.0",
    "org.scalacheck"     %% "scalacheck"               % "1.17.0",
    "junit"              % "junit"                     % "4.13.2",
    "org.testcontainers" % "kafka"                     % "1.17.6"
  ).map(_ % Test)

  val libraryDependencies: Seq[ModuleID] = Seq(
    "org.apache.kafka"           % "kafka-clients"         % "3.4.0",
    "com.typesafe"               % "config"                % "1.4.2",
    "io.circe"                   %% "circe-generic"        % "0.14.5",
    "com.typesafe.scala-logging" %% "scala-logging"        % "3.9.5",
    "org.apache.spark"           %% "spark-streaming"      % "3.5.0",
    "org.apache.spark"           %% "spark-sql"            % "3.5.0",
    "org.apache.spark"           %% "spark-sql-kafka-0-10" % "3.5.0",
    "org.apache.spark"           %% "spark-core"           % "3.5.0"
  ) ++ testDependencies

}
