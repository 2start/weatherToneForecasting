package eu.streamline.hackathon.flink.scala.job

import java.time.Instant
import java.util.Date

import eu.streamline.hackathon.GDELTEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object FlinkScalaJob {

  case class TempRule(name: String, threshold: Integer)

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val pathToWeather = parameters.get("wpath")
    val startWebUI = parameters.getBoolean("webui", false)
    val config = new Configuration()
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = if (startWebUI) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
      else StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new RocksDBStateBackend("file:///tmp/flinkrocksdb", true))


    // join events to weather data on the same day
    val joinWindowDays = 1

    // we can use tumbling or sliding windows here
    val avgToneWindow = TumblingEventTimeWindows.of(Time.days(1))
    // SlidingEventTimeWindows.of(Time.days(4), Time.days(2))

    val tempRules = Array(TempRule("low", 5), TempRule("mid", 25), TempRule("high", Integer.MAX_VALUE))
    val weatherStream = Streams.getWeatherStream(env, pathToWeather)
    val maxTempWeather = weatherStream.filterWith{element => element.element == "TMAX"}

    val gdeltEventStream = Streams.getGDELTEvents(env, pathToGDELT).filter((event: GDELTEvent) => {
      event.actor1Code_countryCode != null &
        // we only have weather data for USA anyway
        event.actor1Code_countryCode == "USA" &
        event.eventGeo_lat != null &
        event.eventGeo_long != null &
        event.isRoot //&
        //event.eventRootCode.equals("14")
    })


    val stationLocator = new StationLocator()
    val eventNearestStations = gdeltEventStream.map(event => {
      (event, stationLocator.nearest(event))
    }).name("Station Locator")


    case class EventWeather(id: Integer, date: Instant, tone: Double, temp: Double)
    // classified event base on temp rules
    case class CEvent(id: Integer, date: Instant, tone: Double, clz: String)

    eventNearestStations.join(maxTempWeather)
      .where(_._2.id).equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.days(joinWindowDays)))
      // temp is given in 10th of celsius
      .apply{(es, weatherRecord) => EventWeather(es._1.globalEventID, es._1.day.toInstant, es._1.avgTone, weatherRecord.datavalue.toDouble / 10)}
      .name("Event-Weather Join")
      .map(ev => {
        val c = tempRules.filter(r => ev.temp < r.threshold).head.name
        CEvent(ev.id, ev.date, ev.tone, c)
      })
      .keyBy(_.clz)
      .window(avgToneWindow)
      .apply { (key: String, window, events, out: Collector[(Instant, String, Double)]) =>
        val avg = events.map(_.tone).sum / events.size
        out.collect((Instant.ofEpochMilli(window.getStart), key, avg))
      }
      .name("AVG Tone")
      .writeAsCsv("all-weather-tone.csv", WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Flink Scala GDELT Analyzer")
  }
}

