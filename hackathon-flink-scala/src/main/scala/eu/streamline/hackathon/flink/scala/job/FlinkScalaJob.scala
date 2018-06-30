package eu.streamline.hackathon.flink.scala.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.extensions._
import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.io.{CsvInputFormat, TupleCsvInputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkScalaJob {

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val pathToWeather = parameters.get("wpath")
    val country = parameters.get("country", "USA")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
      * Load weather dataset
      *
      */

    val weatherRawStream =
      env.readTextFile(pathToWeather).setParallelism(1)

    case class WeatherRecord(id: String, date: Date, element: String, datavalue: String)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    val weatherStream = weatherRawStream.mapWith(line => {
      val elements = line.split(",")
      val elementTuple = (elements(0), dateFormat.parse(elements(1)), elements(2), elements(3))
      WeatherRecord.tupled(elementTuple)
    })


    val weatherTimestampedStream = weatherStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WeatherRecord](Time.seconds(0)) {
      override def extractTimestamp(element: WeatherRecord): Long = {
        element.date.getTime
      }
    })

    // keyBy station, date
    val maxTempWeather = weatherTimestampedStream
      .filterWith{element => element.element == "TMAX"}

//    maxTempWeather.print()


    implicit val typeInfo = createTypeInformation[GDELTEvent]
    implicit val dateInfo = createTypeInformation[Date]

    val gdeltStream = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

    val filteredStream = gdeltStream.filter((event: GDELTEvent) => {
        event.actor1Code_countryCode != null &
        event.actor1Code_countryCode == country &
        event.eventGeo_lat != null &
        event.eventGeo_long != null
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.dateAdded.getTime
      }
    })

    val stationLocator = new StationLocator()

    val eventNearestStations = filteredStream.map(event => {
      (event, stationLocator.nearest(event))
    })

//    eventNearestStations.print()

    eventNearestStations.join(maxTempWeather)
      .where(eventWithStation => eventWithStation._2.id)
      .equalTo(record => record.id)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .apply{(eventWithStation, weatherRecord) => (eventWithStation._1, weatherRecord.datavalue)}
    .print()

    env.execute("Flink Scala GDELT Analyzer")

  }

}
