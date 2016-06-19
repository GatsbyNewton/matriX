name := "Matrix"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++={
  val sparkVersion = "1.4.1"
  Seq(
    "org.apache.hadoop" % "hadoop-client" % "2.4.1",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.scalanlp" %% "breeze" % "0.11.1",
    "org.scalanlp" %% "breeze-natives" % "0.11.1",
    "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test",
    "colt" % "colt" % "1.2.0",
    "com.github.fommil.netlib" % "all" % "1.1.2"
  )
}

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "netlib Repository" at "http://repo1.maven.org/maven2/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"