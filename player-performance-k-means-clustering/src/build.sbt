val scalaVersion1 = "2.13.10" // or "2.12.15"

lazy val root = project
  .in(file("."))
  .settings(
    name := "nba-player-clustering",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := "2.13.10",

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.5.1" % "provided",
      "org.scala-lang" % "scala-reflect" % "2.13.14" % "provided",
      "com.github.tototoshi" %% "scala-csv" % "1.3.10",
      "org.scalanlp" %% "breeze" % "1.2",
      "org.scalanlp" %% "breeze-viz" % "1.2"
    )
  )