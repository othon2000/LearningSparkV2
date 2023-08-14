package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, desc, expr, when, year, month, date_format}

object Example3_FireDepartment {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Example3_FireDepartment")
      .getOrCreate()

    if (args.length <= 0) {
      println("usage Example3_FireDepartment <file path to csv")
      System.exit(1)
    }

    //schema
    val ddlSchemaStr = """`CallNumber` STRING,`UnitID` STRING,`IncidentNumber` STRING,`CallType` STRING,`CallDate` DATE,
                          `WatchDate` DATE,`CallFinalDisposition` STRING,`AvailableDtTm` TIMESTAMP,`Address` STRING,
                          `City` STRING,`Zipcode` STRING,`Battalion` STRING,`StationArea` STRING,`Box` STRING,
                          `OriginalPriority` STRING,`Priority` STRING,`FinalPriority` STRING,`ALSUnit` STRING,
                          `CallTypeGroup` STRING,`NumAlarms` INT,`UnitType` STRING,`UnitSequenceInCallDispatch` STRING,
                          `FirePreventionDistrict` STRING,`SupervisorDistrict` STRING,`Neighborhood` STRING,
                          `Location` STRING,`RowID` STRING,`Delay` DECIMAL"""
    val schemaDdl = StructType.fromDDL(ddl = ddlSchemaStr)

    //get the path to the JSON file
    val csvFile = args(0)
    val fireDf = spark.read
                      .option("header", true)
                      .schema(schemaDdl)
                      .option("dateFormat", "MM/dd/yyyy")
                      .option("timeStampFormat", "MM/dd/yyyy hh:mm:ss a")
                      .csv(csvFile)
                      //sanitize null CallTypeGroup values in some rows where CallType contains fire
                      .withColumn("CallTypeGroup", 
                        when(col("CallTypeGroup").isNull && col("CallType").contains("Fire"), "Fire").otherwise(col("CallTypeGroup")))

    fireDf.schema.printTreeString()
    println(s"schema string: [${fireDf.columns.mkString(",")}]")
    fireDf.show(10, false)

    println("distinct call types with counts")
    val fireDfAggCallType = fireDf.select("CallType", "CallTypeGroup")
          .groupBy("CallType", "CallTypeGroup")
          .count()
          .orderBy(desc("count"))
          .show(200, false)
    
    // fireDfAggCallType.write.csv("../out/fireDfAggCallType.csv")
    // fireDfAggCallType.write.json("../out/fireDfAggCallType.json")
    // fireDfAggCallType.write.parquet("../out/fireDfAggCallType.parquet")

    println("different types of fire calls in 2018")
    fireDf.select("CallType")
          .where(year(col("CallDate")) === 2018 && col("CallTypeGroup") === "Fire")
          .groupBy("CallType")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("what months in 2018 saw the highest number of fire calls")
    fireDf.select(date_format(col("CallDate"), "yyyy/MMM").as("month"))
          .where(year(col("CallDate")) === 2018 && col("CallTypeGroup") === "Fire")
          .groupBy("month")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("web UI still running, press any key to terminate")
    System.in.read()

  }
}
