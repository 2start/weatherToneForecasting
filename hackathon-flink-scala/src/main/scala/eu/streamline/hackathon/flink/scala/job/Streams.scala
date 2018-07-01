package eu.streamline.hackathon.flink.scala.job

import java.text.SimpleDateFormat

import eu.streamline.hackathon.{GDELTEvent, GDELTInputFormat, WeatherRecord}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._

/**
  *
  * @author Manuel Hotz &lt;manuel.hotz&gt;
  * @since 1.0
  */
object Streams {
  def getGDELTEvents(env: StreamExecutionEnvironment, pathToGDELT: String) : DataStream[GDELTEvent] = {
    env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)
      .name("GDELT Event Source")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.day.getTime
      }
    })
  }


  def getWeatherStream(env: StreamExecutionEnvironment, pathToWeather: String) : DataStream[WeatherRecord] = {

    val weatherRawStream = env.readTextFile(pathToWeather).setParallelism(1).name("Weather Source")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    weatherRawStream.mapWith(line => {
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
  }
}
