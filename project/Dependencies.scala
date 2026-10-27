import sbt._

object Dependencies {
  lazy val chrononVersion = "0.0.115"

  lazy val chrononApi       = "ai.chronon"   %% "api"                    % chrononVersion
  lazy val chrononOnline    = "ai.chronon"   %% "online"                 % chrononVersion
  lazy val aerospikeClient  = "com.aerospike" % "aerospike-client-jdk8"  % "10.4.0"
  lazy val scallop          = "org.rogach"   %% "scallop"                % "4.1.0"
  lazy val sparkSql         = "org.apache.spark" %% "spark-sql"          % "3.5.3"
  lazy val logback          = "ch.qos.logback" % "logback-classic"       % "1.2.13"
  lazy val slf4j            = "org.slf4j"      % "slf4j-api"             % "1.7.36"
  lazy val scalatest        = "org.scalatest" %% "scalatest"            % "3.2.17"

}
