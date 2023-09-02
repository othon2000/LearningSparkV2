package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Timestamp


object Example3_FireDepartmentDataset {

  case class EmergencyEntry(
      callNumber: String,
      unitID: String,
      incidentNumber: String,
      callType: String,
      callDate: Timestamp,
      watchDate: Timestamp,
      callFinalDisposition: String,
      availableDtTm: Timestamp,
      address: String,
      city: String,
      zipcode: String,
      battalion: String,
      stationArea: String,
      box: String,
      originalPriority: String,
      priority: String,
      finalPriority: String,
      aLSUnit: String,
      callTypeGroup: String,
      numAlarms: Int,
      unitType: String,
      unitSequenceInCallDispatch: String,
      firePreventionDistrict: String,
      supervisorDistrict: String,
      neighborhood: String,
      location: String,
      rowID: String,
      delay: BigDecimal
  )

  // schema is still needed because of fields which are not string (Timestamp, Decimal)
  val ddlSchemaStr = """`CallNumber` STRING,`UnitID` STRING,`IncidentNumber` STRING,`CallType` STRING,`CallDate` DATE,
                        `WatchDate` DATE,`CallFinalDisposition` STRING,`AvailableDtTm` TIMESTAMP,`Address` STRING,
                        `City` STRING,`Zipcode` STRING,`Battalion` STRING,`StationArea` STRING,`Box` STRING,
                        `OriginalPriority` STRING,`Priority` STRING,`FinalPriority` STRING,`ALSUnit` STRING,
                        `CallTypeGroup` STRING,`NumAlarms` INT,`UnitType` STRING,`UnitSequenceInCallDispatch` STRING,
                        `FirePreventionDistrict` STRING,`SupervisorDistrict` STRING,`Neighborhood` STRING,
                        `Location` STRING,`RowID` STRING,`Delay` DECIMAL"""

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Example3_FireDepartmentDataset")
      .getOrCreate()

    import spark.implicits._

    if (args.length <= 0) {
      println("usage Example3_FireDepartment <file path to csv")
      System.exit(1)
    }

    //get the path to the JSON file
    val csvFile = args(0)
    val fireDf = spark.read
                      .option("header", true)
                      .schema(ddlSchemaStr)
                      .option("dateFormat", "MM/dd/yyyy")
                      .option("timeStampFormat", "MM/dd/yyyy hh:mm:ss a")
                      .csv(csvFile)
                      //sanitize null CallTypeGroup values in some rows where CallType contains fire
                      .withColumn("CallTypeGroup", 
                        when($"CallTypeGroup".isNull && $"CallType".contains("Fire"), "Fire").otherwise($"CallTypeGroup"))
                      .as[EmergencyEntry]

    fireDf.schema.printTreeString()
    println(s"schema string: [${fireDf.columns.mkString(",")}]")
    fireDf.show(10, false)

    println("distinct call types with counts")
    val fireDfAggCallType = fireDf.select("CallType", "CallTypeGroup")
          .groupBy("CallType", "CallTypeGroup")
          .count()
          .orderBy(desc("count"))
          .show(200, false)
    
    println("different types of fire calls in 2018")
    fireDf.filter({e => e.callTypeGroup == "Fire" && e.callDate.toLocalDateTime.getYear == 2018})
          .map({e => e.callType})
          .groupBy("value") //to have a different name to the column we need to specify a schema
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("what months in 2018 saw the highest number of fire calls")
    fireDf.filter({e => e.callTypeGroup == "Fire" && e.callDate.toLocalDateTime.getYear == 2018})
          .map({e => s"2018/${e.callDate.toLocalDateTime.getMonth}"})
          .groupBy("value") //to have a different name to the column we need to specify a schema
          .count()
          .orderBy(desc("count"))
          .show(false)

    println("which neighborhoods that generated the most fire calls in 2018")
    fireDf.filter({e => e.callTypeGroup == "Fire" && e.callDate.toLocalDateTime.getYear == 2018})
          .map({e => e.neighborhood})
          .groupBy("value") //to have a different name to the column we need to specify a schema
          .count()
          .orderBy(desc("count"))
          .show(false)

//     println("which week in 2018 had the most fire calls")
//     fireDf.select(concat(year($"CallDate"), typedlit("/CW"), weekofyear($"CallDate")).as("week"))
//           .where(year($"CallDate") === 2018 && $"CallTypeGroup" === "Fire")
//           .groupBy("week")
//           .count()
//           .orderBy(desc("count"))
//           .show(false)

//     println("is there a correlation between neighborhood, zip code, and number of fire calls?")
//     //TODO: solve

//     println("how can we use parquet files or SQL tables to store this data and then read it back?")
//     //TODO: solve




    println("web UI still running, press ENTER to terminate")
    System.in.read()

  }
}
