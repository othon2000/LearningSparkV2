package main.scala.chapter8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object Example8 {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Example-8")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5) //default is 200

    spark.streams.addListener(getListener())

    val lines = spark.readStream
                     .format("socket")
                     .option("host", "localhost")
                     .option("port", 9999)
                     .load()
    
    val words = lines.select(explode(split(col("value"), "\\s")).as("word"))
    val counts = words.groupBy("word")
                      .count()
                      .orderBy(desc("count"))

    // val checkpointDir = "..."

    val streamingQuery = counts.writeStream
                               .format("console")
                               .outputMode("complete")
                               .trigger(Trigger.ProcessingTime("1 second"))
                              //  .option("checkpointLocation", checkpointDir)
                               .start()

    streamingQuery.awaitTermination()

  }

  private def getListener(): StreamingQueryListener = {
    new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"query started [${event.id}]")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        // quite verbose, uncomment for testing
        //println(s"query made progress [${event.progress}]")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"query terminated [${event.id}]")
      }
    }
  }
}
