package eu.streamline.hackathon.flink.scala.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.extensions._
import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.common.accumulators.Histogram
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.io.{CsvInputFormat, TupleCsvInputFormat}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkScalaJob {

  case class WeatherRecord(id: String, date: Date, element: String, datavalue: String)

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val pathToWeather = parameters.get("wpath")
    val country = parameters.get("country", "USA")

    val config = new Configuration()
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    // val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val weatherRawStream = env.readTextFile(pathToWeather).setParallelism(1)


    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val weatherStream = weatherRawStream.mapWith(line => {
      val elements = line.split(",")
      val elementTuple = (elements(0), dateFormat.parse(elements(1)), elements(2), elements(3))
      WeatherRecord.tupled(elementTuple)
    })
    .filter(weatherRecord => weatherRecord.date.after(dateFormat.parse("20170131")))

    val weatherTimestampedStream = weatherStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[WeatherRecord](Time.seconds(0)) {
      override def extractTimestamp(element: WeatherRecord): Long = {
        element.date.getTime
      }
    })

    // keyBy station, date
    val maxTempWeather = weatherTimestampedStream.filterWith{element => element.element == "TMAX"}

//    val ot = OutputTag[WeatherRecord]("TMAX Value")
//    val maxTempSide = maxTempWeather.getSideOutput(ot)
//      .timeWindowAll(Time.days(30))
//      .apply((window: TimeWindow, values: Iterable[WeatherRecord], out: Collector[(Date, Double)])=>{
//        val vd = values.map(_.datavalue.toDouble)
//        val avg = vd.sum / values.size
//
//        out.collect((values.head.date, avg))
//        })

    implicit val typeInfo = createTypeInformation[GDELTEvent]
    implicit val dateInfo = createTypeInformation[Date]

    val gdeltStream = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

    val filteredStream = gdeltStream.filter((event: GDELTEvent) => {
        event.actor1Code_countryCode != null &
        event.actor1Code_countryCode == country &
        event.eventGeo_lat != null &
        event.eventGeo_long != null &
        event.isRoot &
        event.eventRootCode.equals("14")
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.day.getTime
      }
    })

    val stationLocator = new StationLocator()

    val eventNearestStations = filteredStream.map(event => {
      (event, stationLocator.nearest(event))
    })

    eventNearestStations.join(maxTempWeather)
      .where(eventWithStation => eventWithStation._2.id.trim)
      .equalTo(record => record.id.trim)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .apply{(es, weatherRecord) => (es._1.globalEventID, es._1.day, es._1.eventRootCode, Math.round(weatherRecord.datavalue.toDouble / 100))}
      .map(e => (e, 1))
      .keyBy(ev => ev._1._4)
      .timeWindow(Time.days(1))
      .reduceWith (
        (e1, e2) => (e1._1, e1._2 + e2._2)
      )
      .map(ecount => (ecount._1._4 * 10, ecount._2, ecount._1._2))
      .print()

    env.execute("Flink Scala GDELT Analyzer")

  }
}
