ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.zetaris"

import NativePackagerHelper._

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging, UniversalPlugin)
  .settings(
    name := "perf-test-generator",

    // Library dependencies
    libraryDependencies ++= {
      val sparkVersion = "3.5.1"
      val deltaVersion = "3.2.0"
      val hadoopVersion = "3.3.4"
      val awsVersion = "1.12.262"

      Seq(
        // Spark Core Dependencies
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

        // Delta Lake
        "io.delta" %% "delta-spark" % deltaVersion,

        // AWS and Hadoop for S3
        "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
        "com.amazonaws" % "aws-java-sdk-bundle" % awsVersion,

        // Hadoop Client (for S3A filesystem)
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion,

        // Logging
        "ch.qos.logback" % "logback-classic" % "1.4.11",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

        // Configuration
        "com.typesafe" % "config" % "1.4.2",

        // Command line parsing
        "com.github.scopt" %% "scopt" % "4.1.0",

        // Test dependencies
        "org.scalatest" %% "scalatest" % "3.2.17" % Test,
        "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
        "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
      )
    },

    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard"
    ),

    // Assembly plugin settings
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", "maven", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => xs.map(_.toLowerCase) match {
        case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil => MergeStrategy.discard
        case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") => MergeStrategy.discard
        case "services" :: _ => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
      case x if x.endsWith(".proto") => MergeStrategy.rename
      case x if x.contains("hadoop") => MergeStrategy.first
      case x if x.contains("jersey") => MergeStrategy.first
      case x if x.contains("parquet") => MergeStrategy.first
      case x if x.contains("avro") => MergeStrategy.first
      case _ => MergeStrategy.first
    },

    // Assembly JAR name
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",

    // Exclude Scala library from assembly (since Spark provides it)
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),

    // Test settings
    Test / fork := true,
    Test / parallelExecution := false,

    // Runtime settings
    run / fork := true,
    run / connectInput := true,

    // JVM options
    javaOptions ++= Seq(
      "-Xmx4G",
      "-XX:+UseG1GC",
      "-XX:+UseStringDeduplication"
    ),

    // Dependency resolution settings
    resolvers ++= Seq(
      "Spark Packages Repo" at "https://repos.spark-packages.org/",
      "Maven Central" at "https://repo1.maven.org/maven2/",
      "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
    ),

    // Exclude problematic transitive dependencies
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("log4j", "log4j")
    ),
    
    // Native Packager settings for distribution
    Universal / mappings ++= {
      // Include the assembled JAR in lib/
      val fatJar = (Compile / assembly).value
      Seq(fatJar -> s"lib/${fatJar.getName}")
    },
    
    // Include configuration files
    Universal / mappings ++= directory("conf"),
    
    // Include scripts
    Universal / mappings ++= Seq(
      file("bin/start-generator.sh") -> "bin/start-generator.sh"
    ),
    
    // Exclude Scala library from lib (included in assembly)
    Universal / mappings := {
      val origMappings = (Universal / mappings).value
      origMappings.filterNot { case (_, target) =>
        target.startsWith("lib/") && target.contains("scala-library")
      }
    },
    
    // Package name
    Universal / packageName := s"${name.value}-${version.value}",
    
    // Main class
    Compile / mainClass := Some("com.zetaris.testgen.DeltaLakeBMTApplication"),
    
    // JVM options for the application
    Universal / javaOptions ++= Seq(
      "-J-Xmx8g",
      "-J-XX:+UseG1GC",
      "-Dlog4j.configuration=file:conf/log4j.properties"
    )
  )

// Add aliases for common tasks
addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
addCommandAlias("testAll", "test")
addCommandAlias("buildJar", "assembly")
addCommandAlias("packageDist", "universal:packageBin")
addCommandAlias("packageZip", "universal:packageZipTarball")