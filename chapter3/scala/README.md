### How to build the package
 1. sbt clean package

### How to run the Example
To run the Scala code for this chapter use:

 * `spark-submit --class main.scala.chapter3.Example3_7 target/scala-2.12/main-scala-chapter3_2.12-1.0.jar data/blogs.json`
 * `spark-submit --class main.scala.chapter3.Example3_FireDepartmentDataFrame target/scala-2.12/main-scala-chapter3_2.12-1.0.jar ../data/sf-fire-calls.csv`
 * `spark-submit --class main.scala.chapter3.Example3_FireDepartmentDataset target/scala-2.12/main-scala-chapter3_2.12-1.0.jar ../data/sf-fire-calls.csv`
