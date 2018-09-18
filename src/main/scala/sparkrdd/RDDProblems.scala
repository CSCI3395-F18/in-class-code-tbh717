package sparkrdd

import org.apache.spark.rdd.RDD
import java.time.LocalDate

import org.apache.spark.{SparkConf, SparkContext}

import scalafx.application.JFXApp
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer._
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.SwingRenderer

case class DataEntry(stationID: String, date: LocalDate, dataType: String, value: Int,
                     measurement: Option[String], quality: Option[String], time: Option[Int])
case class StationData(id: String, lat: Double, long: Double, elev: Double,
                       state: Option[String], name: String)

object RDDProblems extends App {

    def getOptionalString(line: String): Option[String] = {
        if(line == "") None else Some(line)
    }
    def getOptionalInt(line: String): Option[Int] = {
        if(line == "") None else {
            try {
                val num = line.toInt
                Some(num)
            }
            catch {
                case _: NumberFormatException => None
            }
        }
    }

    def parseLine(line: String): DataEntry = {
        def getDate(line: String): LocalDate = {
            val year = line.substring(0,4).toInt
            val month = line.substring(4,6).toInt
            val day = line.substring(6,8).toInt

            LocalDate.of(year, month, day)
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

    def parseCountryLine(line: String, byId: Boolean): (String, String) = {
        val id = line.substring(0,2)
        val name = line.substring(3).trim

        if(byId) id -> name else name -> id
    }

    def parseStationLine(line: String): StationData = {
        StationData(
            line.substring(0,11).trim,
            line.substring(12,20).trim.toDouble,
            line.substring(21,30).trim.toDouble,
            line.substring(31,37).trim.toDouble,
            getOptionalString(line.substring(38,40).trim),
            line.substring(41,71).trim
        )
    }

    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
    val dateTag = scala.reflect.classTag[LocalDate]

    /* SPARK CODE */

    val conf = new SparkConf()
        .setAppName("Temp Data")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    /* MAIN CODE */

    val countriesById = sc.textFile("data/sparkrdd/countries.txt")
        .map(parseCountryLine(_, byId = true))
        .collect.toMap
    val countriesByName = sc.textFile("data/sparkrdd/countries.txt")
        .map(parseCountryLine(_, byId = false))
        .collect.toMap

    val stations = sc.textFile("data/sparkrdd/stations.txt")
        .map(parseStationLine)

    val observations = sc.textFile("data/sparkrdd/2017.csv").zipWithIndex().collect {
        case (elem, index) if index != 0 => elem
    }.map(parseLine)

    /* PROBLEMS */

    // How many stations are there in the state of Texas?
    def problem1(): Unit = {
        val numStations = stations.filter(s => s.state.getOrElse("") == "TX").count
        println(numStations)
    }

    // How many of those stations have reported some form of data in 2017?
    def problem2(): Unit = {
        val texasStations = stations.filter(s => s.state.getOrElse("") == "TX")
            .map(_.id)
        val num2017 = texasStations.intersection(observations.map(_.stationID)).count
        println(num2017)

    }

    // What is the highest temperature reported anywhere this year? Where was it and when?
    def problem3(): Unit = {
        val highestTempObs = observations.filter(_.dataType == "TMAX")
            .reduce((o1,o2) => if (o1.value > o2.value) o1 else o2)
        val station = stations.filter(_.id == highestTempObs.stationID).first()
        println("OBSERVATION: " + highestTempObs)
        println("STATION:     " + station)
    }

    // How many stations in the stations list haven't reported any data in 2017?
    def problem4(): Unit = {
        val stations2017 = observations.map(_.stationID)
        val stationsById = stations.map(_.id)

        val numStationsAbsent = stationsById.subtract(stations2017).count

        println(numStationsAbsent)
    }

    // What is the maximum rainfall for any station in Texas during 2017?
    // What station and when?
    def problem5(): Unit = {
        val texasStations = stations.filter(s => s.state.getOrElse("") == "TX")
            .map(_.id).collect()
        val maxRainfallObs = observations
            .filter(_.dataType == "PRCP")
            .filter(o => texasStations.contains(o.stationID))
            .reduce((o1, o2) => if(o1.value > o2.value) o1 else o2)

        val station = stations.filter(_.id == maxRainfallObs.stationID).first()
        println("OBSERVATION: " + maxRainfallObs)
        println("STATION:     " + station)
    }

    // What is the maximum rainfall for any station in India during 2017?
    // What station and when?
    def problem6(): Unit = {
        val idIndia = countriesByName("India")
        val maxRainfallObs = observations
            .filter(o => o.stationID.substring(0,2) == idIndia && o.dataType == "PRCP")
            .reduce((o1, o2) => if(o1.value > o2.value) o1 else o2)

        val station = stations.filter(_.id == maxRainfallObs.stationID).first()
        println("OBSERVATION: " + maxRainfallObs)
        println("STATION:     " + station)
    }

    def getSAStations(): RDD[StationData] = {
        stations.filter(s => s.state.getOrElse("") == "TX" && (s.name.toLowerCase.contains("san antonio") || s.name.toLowerCase.contains("sanantonio")))
    }

    // How many weather stations are there associated with San Antonio, TX?
    def problem7(): Unit = {
        val saStations = getSAStations()
        val count = saStations.count()

        println(count)
    }

    // How many of those have reported temperature data in 2017?
    def problem8(): Unit = {
        val tempDataTypes = Array(
            "TMAX", "TMIN", "MDTN", "MDTX", "MNPN", "MXPN", "TOBS"
        )
        val saStations = getSAStations()
        val saStations2017 =  saStations.map(_.id).intersection(
            observations
                .filter(o => tempDataTypes.contains(o.dataType))
                .map(_.stationID)
        )
        val count = saStations2017.count

        println(count)
    }

    // What is the largest daily increase in high temp for San Antonio in this data file?
    def problem9(): Unit = {
        val x = LocalDate.of(2017,12,30).toEpochDay
        val saStations = getSAStations().map(_.id).collect()

        // Returns map of pair type (Date, avg of TMAX from all stations)
        val tempData = observations
            .filter(o => saStations.contains(o.stationID))
            .filter(_.dataType == "TMAX")
            .groupBy(_.date)
            .map(pair => {
                val sum = pair._2.map(_.value).sum.toFloat
                val avg = sum/pair._2.size.toFloat
                val epochDay = pair._1.toEpochDay
                (epochDay, avg)
            })
            .collect().toMap

        // Loop through all days of 2017
        val startDate = LocalDate.of(2017,1,1).toEpochDay
        val endDate = LocalDate.of(2017,12,31).toEpochDay
        var biggestIncrease = (startDate, 0.0)
        for(day <- startDate to endDate-1) {
            val avg = tempData.get(day)
            val nextAvg = tempData.get(day+1)
            if(avg.isDefined && nextAvg.isDefined) {
                val diff = nextAvg.get - avg.get
                if(diff > biggestIncrease._2) {
                    biggestIncrease = (day,diff)
                }
            }
        }

        // Print values
        println("BIGGEST INCREASE: ")
        println("From "
            + LocalDate.ofEpochDay(biggestIncrease._1)
            + "at " + tempData(biggestIncrease._1))
        println("To "
            + LocalDate.ofEpochDay(biggestIncrease._1+1)
            + "at " + tempData(biggestIncrease._1+1))
        println("of difference " + biggestIncrease._2)

    }

    // What is the correlation coefficient between high temperatures and rainfall for
    // San Antonio? Note that you can only use values from the same date
    // and station for the correlation.
    def problem10(): Unit = {
        def suitableStation(): RDD[String] = {
            val saStations = getSAStations()
            // Returns map of type (stationID, [observations])
            val dataByStation = observations
                .groupBy(_.stationID)
                .map(pair => pair._1 -> (pair._2.filter(_.dataType == "TMAX"), pair._2.filter(_.dataType == "PRCP")))
            // Find station with the most data, returned as (stationId, [observations])
            dataByStation.filter(pair => pair._2._1.size >= 365 && pair._2._2.size >= 365).map(_._1)
        }

        /*
         * DATA GATHERING
         */
        // getSAStations().foreach(println)
        val stationId = "USW00012921" // findSuitableStation() -> hardcoded, for speed
        // Find relevant observations
        val stationData = observations
            .filter(o => o.stationID == stationId)
        // Separate data into Map(date -> TMAX) and Map(date -> PRCP)
        val tempData = stationData
            .filter(_.dataType == "TMAX")
            .map(o => o.date.toEpochDay -> o.value)
        val rainData = stationData
            .filter(_.dataType == "PRCP")
            .map(o => o.date.toEpochDay -> o.value)
        // Map of all days
        val startDate = LocalDate.of(2017,1,1).toEpochDay
        val endDate = LocalDate.of(2017,12,31).toEpochDay
        val commonDates = (startDate to endDate).toArray
        // Get map of date -> val
        val tempMap = tempData.collect.toMap
        val rainMap = rainData.collect.toMap

        /*
         * CORRELATION
         */
        val tempVals = tempMap.values
        val rainVals = rainMap.values
        // Sums
        val sumTemp = tempVals.sum.toLong
        val sumRain = rainVals.sum.toLong
        // Square sums
        val sumTempSq = tempVals.foldLeft(0.0)((acc, e) => acc + Math.pow(e,2).toLong)
        val sumRainSq = rainVals.foldLeft(0.0)((acc, e) => acc + Math.pow(e,2).toLong)
        // Sum of products
        val sumProd = commonDates.reduceLeft((acc, e) => acc + (tempMap(e) * rainMap(e)))
        // Get Pearson score
        val sz = commonDates.length
        val numerator = sumProd - (sumTemp*sumRain/sz)
        val denominator = Math.sqrt(
            (sumTempSq - Math.pow(sumTemp,2)/sz) * (sumRainSq - Math.pow(sumRain,2)/sz)
        )
        println("NUMERATOR:   " + numerator)
        println("DENOMINATOR: " + denominator)
        println("SCORE:       " + numerator/denominator)
    }

    // Make a plot of temperatures over time for five different stations,
    // each separated by at least 10 degrees in latitude.
    // Make sure you tell me which stations you are using.
    def problem11(): Unit = {

        // Prints out stations with lots of data
        def findStations(): Unit = {
            val stationMap = stations
                .map(s => s.id -> s)
                .collect
                .toMap

            var n = 0
            observations
                .filter(_.dataType == "TMAX")
                .groupBy(_.stationID)
                .foreach(pair => {
                    val station = stationMap(pair._1)
                    // Ensure that it has data for each day in 2017
                    if(pair._2.size >= 365) println(station)
                })
        }

        def cToF(c: Double): Double = c * 9.0/5.0 + 32

        /* Selected stations, from findStations()
        * 1. StationData(ASN00006105,-25.8925,113.5772,33.8,None,SHARK BAY AIRPORT)
        * 2. StationData(ASN00090182,-37.583,141.3339,130.6,None,CASTERTON)
        * 3. StationData(RMW00040604,8.7333,167.7333,2.1,Some(MH),KWAJALEIN)
        * 4. StationData(USC00425182,41.735,-111.8564,1364.0,Some(UT),LOGAN RADIO KVNU)
        * 5. StationData(FIE00143316,62.3339,21.1939,5.0,None,KASKINEN SALGRUND)
        */

        /* Select 5 stations */
        val selectedStationIds = Array(
            "ASN00006105",
            "ASN00090182",
            "RMW00040604",
            "USC00425182",
            "FIE00143316"
        )
        // val selectedStations = selectedStationIds.map(id => stationMap(id))

        /* Create data structure of type [Map(date -> TMAX in F), ...]*/
        val stationTempMaps = selectedStationIds
            .map(id => observations.filter(obs => obs.stationID == id && obs.dataType == "TMAX"))
            .map(obsRDD => {
                obsRDD
                    .map(o => o.date.toEpochDay.toInt -> cToF(o.value/10))
                    .collect.toMap
            })

        /* Create array of ordered dates - all days from 2017 */
        val startDate = LocalDate.of(2017,1,1).toEpochDay.toInt
        val endDate = LocalDate.of(2017,12,31).toEpochDay.toInt
        val dayRange = (startDate to endDate).toList
//        val dayRange = new Array[Int](365)
//        var idx = 0
//        for(day <- startDate to endDate) {
//            dayRange(idx) = day.toInt
//            idx += 1
//        }

        /* CONSTRUCT GRAPH */
        val stationTempsArrs = stationTempMaps
            .map(tempsMap => dayRange.map(day => tempsMap(day)))
        val graphDayRange = (1 to (endDate - startDate)+1).toList

        val plot = Plot.scatterPlots(
            Seq(
                (graphDayRange, stationTempsArrs(0), GreenARGB, 6),
                (graphDayRange, stationTempsArrs(1), RedARGB, 6),
                (graphDayRange, stationTempsArrs(2), BlueARGB, 6),
                (graphDayRange, stationTempsArrs(3), MagentaARGB, 6),
                (graphDayRange, stationTempsArrs(4), CyanARGB, 6)
            ), title = "Temperatures across 2017 from 5 stations", xLabel = "Days", yLabel = "Max temperature")
        SwingRenderer(plot, 1000, 1000, true)
    }

    /* EXECUTE PROBLEMS */

    // problem1()
    // problem2()
    // problem3()
    // problem4()
    // problem5()
    // problem6()
    // problem7()
    // problem8()
    problem9()
    // problem10()
    // problem11()

    sc.stop()
}
