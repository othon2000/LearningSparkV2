### How to build the package
 1. sbt clean package

### How to run the Example
To run the Scala code for this chapter use:

Terminal 1:
 * `nc -lk 9999`
 * use this same terminal to type or paste text which will be used by the streaming job

Terminal 2:
 * only after opening the socket in Terminal 1
 * `spark-submit --class main.scala.chapter8.Example8 target/scala-2.12/main-scala-chapter8_2.12-1.0.jar`
