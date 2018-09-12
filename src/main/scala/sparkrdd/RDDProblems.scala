package sparkrdd

import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}

case class DataDate(year: Int, month: Int, day: Int)
case class DataEntry(stationID: String, date: DataDate, dataType: String, value: Int,
                     measurement: Option[String], quality: Option[String], time: Option[Int])

object RDDProblems extends App {

    def parseLine(line: String): DataEntry = {
        def getDate(line: String): DataDate = {
            val year = line.substring(0,4).toInt
            val month = line.substring(4,6).toInt
            val day = line.substring(6,8).toInt

            DataDate(year, month, day)
        }
        def getOptionalString(line: String): Option[String] = {
            if(line == "") None else Some(line)
        }
        def getOptionalInt(line: String): Option[Int] = {
            if(line == "") None else Some(line.toInt)
        }

        val csvRegex = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)"
        val arr = line.split(csvRegex)

        DataEntry(
            arr(0),
            getDate(arr(1)),
            arr(2),
            arr(3).toInt,
            getOptionalString(arr(4)),
            getOptionalString(arr(5)),
            getOptionalInt(arr(6))
        )
    }

    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // CODE HERE
    val countries = Source.fromFile("data/sparkrdd/countries.txt").getLines.map(
        line => line.substring(0,2) -> line.substring(3).trim).toMap

    val lines = sc.textFile("data/sparkrdd/2017.csv").zipWithIndex().collect {
        case (elem, index) if index != 0 => elem
    }.map(parseLine)

    // val testElem = lines.first()
    // println(testElem)

    sc.stop()
}
