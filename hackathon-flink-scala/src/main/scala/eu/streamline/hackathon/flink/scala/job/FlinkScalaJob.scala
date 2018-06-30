package eu.streamline.hackathon.flink.scala.job

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object FlinkScalaJob {

  case class WeatherRecord(id: String, date: Date, element: String, datavalue: String)

  case class TempRule(name: String, threshold: Integer)

  def main(args: Array[String]): Unit = {

    // join events to weather data on the same day
    val joinWindowDays = 1

    // we can use tumbling or sliding windows here
    val avgToneWindow = TumblingEventTimeWindows.of(Time.days(3))
      // SlidingEventTimeWindows.of(Time.days(4), Time.days(2))

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val pathToWeather = parameters.get("wpath")
    val startWebUI = parameters.getBoolean("webui", false)

    val config = new Configuration()
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = if (startWebUI) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
      else StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tempRules = Array(TempRule("low", 5), TempRule("mid", 25), TempRule("high", Integer.MAX_VALUE))

    val weatherRawStream = env.readTextFile(pathToWeather).setParallelism(1).name("Weather Source")


    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val weatherStream = weatherRawStream
      .mapWith(line => {
        val elements = line.split(",")
        val elementTuple = (elements(0), dateFormat.parse(elements(1)), elements(2), elements(3))
        WeatherRecord.tupled(elementTuple)
      })
      .filter(weatherRecord => weatherRecord.date.after(dateFormat.parse("20170131")))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WeatherRecord](Time.seconds(0)) {
          override def extractTimestamp(element: WeatherRecord): Long = {
            element.date.getTime
          }
      })

    val maxTempWeather = weatherStream.filterWith{element => element.element == "TMAX"}

    // TODO remove; these typeinfos seem to be not used?
//    implicit val typeInfo = createTypeInformation[GDELTEvent]
//    implicit val dateInfo = createTypeInformation[Date]

    val gdeltEventStream = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)
      .name("GDELT Event Source")
      .filter((event: GDELTEvent) => {
        event.actor1Code_countryCode != null &
        // we only have weather data for USA anyway
        event.actor1Code_countryCode == "USA" &
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

    val eventNearestStations = gdeltEventStream.map(event => {
      (event, stationLocator.nearest(event))
    })

    case class EventWeather(id: Integer, date: Instant, tone: Double, temp: Double)
    // classified event base on temp rules
    case class CEvent(id: Integer, date: Instant, tone: Double, clz: String)

    eventNearestStations.join(maxTempWeather)
      .where(_._2.id).equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.days(joinWindowDays)))
      .apply{(es, weatherRecord) => EventWeather(es._1.globalEventID, es._1.day.toInstant, es._1.avgTone, weatherRecord.datavalue.toDouble / 10)}
      .map(ev => {
        val c = tempRules.filter(r => ev.temp < r.threshold).head.name
        (ev.id, ev.date, ev.tone, c)
      })
      .keyBy(_._4)
      .window(avgToneWindow)
      .apply { (key: String, window, events, out: Collector[(Instant, String, Double)]) =>
        val avg = events.map(_._3.toDouble).sum / events.size
        out.collect((Instant.ofEpochMilli(window.getStart), key, avg))
      }
      .print()

    env.execute("Flink Scala GDELT Analyzer")
  }
}

