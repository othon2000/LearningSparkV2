 //name of the package
 // To build simply type `sbt clean package`
name := "main/java/chapter6"
//version of our package
version := "1.0"
//version of Scala
scalaVersion := "2.12.17"
// spark library dependencies
// change this to Spark 3.0.0 when released
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql"  % "3.4.1"
)
