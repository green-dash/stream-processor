
name := """green-dash-stream-processor"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.6"
val sparkVersion = "1.6.0"

lazy val root = project in file(".")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion exclude ("org.spark-project.spark", "unused"),

    "com.typesafe" % "config" % "1.3.0",
    "com.typesafe.play" %% "play-json" % "2.4.6"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

scalacOptions ++= Seq("-feature")

// A special option to exclude Scala itself form our assembly JAR, since Spark
// already bundles Scala.
assemblyOption in assembly :=
    (assemblyOption in assembly).value.copy(includeScala = false)

