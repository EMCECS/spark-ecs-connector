name := "spark-ecs-s3"
organization := "com.emc.ecs"
version := "1.0-SNAPSHOT"

scalaVersion := "2.10.5"
sparkVersion := "1.6.0"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

sparkComponents := Seq("core", "sql", "mllib")

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "1.6.0",
//  "org.apache.spark" %% "spark-sql" % "1.6.0",
//  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.emc.ecs" % "object-client" % "2.2.0",
  "joda-time" % "joda-time" % "2.9.2",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.scalatest" %% "scalatest" % "2.2.5" % Test
)