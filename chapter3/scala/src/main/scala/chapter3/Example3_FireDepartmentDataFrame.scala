package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Example3_FireDepartmentDataFrame {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Example3_FireDepartmentDataFrame")
      .getOrCreate()

    import spark.implicits._

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
                        when($"CallTypeGroup".isNull && $"CallType".contains("Fire"), "Fire").otherwise($"CallTypeGroup"))

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
          .where(year($"CallDate") === 2018 && $"CallTypeGroup" === "Fire")
          .groupBy("CallType")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("what months in 2018 saw the highest number of fire calls")
    fireDf.select(date_format($"CallDate", "yyyy/MMM").as("month"))
          .where(year($"CallDate") === 2018 && $"CallTypeGroup" === "Fire")
          .groupBy("month")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("which neighborhoods that generated the most fire calls in 2018")
    fireDf.select("Neighborhood")
          .where(year($"CallDate") === 2018 && $"CallTypeGroup" === "Fire")
          .groupBy("Neighborhood")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("which week in 2018 had the most fire calls")
    fireDf.select(concat(year($"CallDate"), typedlit("/CW"), weekofyear($"CallDate")).as("week"))
          .where(year($"CallDate") === 2018 && $"CallTypeGroup" === "Fire")
          .groupBy("week")
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("is there a correlation between neighborhood, zip code, and number of fire calls?")
    //TODO: solve

    println("how can we use parquet files or SQL tables to store this data and then read it back?")
    //TODO: solve




    println("web UI still running, press ENTER to terminate")
    System.in.read()

  }
}
