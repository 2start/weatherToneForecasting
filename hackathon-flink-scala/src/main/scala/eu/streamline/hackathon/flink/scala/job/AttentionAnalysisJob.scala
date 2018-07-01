package eu.streamline.hackathon.flink.scala.job

import java.time.Instant
import java.util.Date

import eu.streamline.hackathon.GDELTEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object AttentionAnalysisJob {

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val startWebUI = parameters.getBoolean("webui", false)
    val config = new Configuration()
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = if (startWebUI) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
      else StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new RocksDBStateBackend("file:///tmp/flinkrocksdb", true))


    val gdeltEventStream = Streams.getGDELTEvents(env, pathToGDELT).map(e => (e, 1L))

    val w = TumblingEventTimeWindows.of(Time.days(2))

    val countries = Seq("US", "UK", "NK", "CH", "IN", "DE", "FR")

    val eventsPerCountry = gdeltEventStream
      .filterWith{ case (ev, count) =>
        ev.eventGeo_countryCode != null & countries.contains(ev.eventGeo_countryCode)
      }
      .keyBy(_._1.eventGeo_countryCode)
      .window(w)
      .apply {
        (key, window, events, out: Collector[(Instant, String, Long)]) =>
          out.collect((Instant.ofEpochMilli(window.getStart), key, events.map(_._2).sum))
      }

    val eventsAll = gdeltEventStream
      .windowAll(w)
      .apply { (window, events, out: Collector[(Instant, Long)]) =>
        out.collect((Instant.ofEpochMilli(window.getStart), events.map(_._2).sum))
      }

    eventsPerCountry.join(eventsAll).where(_._1).equalTo(_._1).window(w)
      .apply { (c, all) =>
        (c._1, c._2, c._3.toDouble / all._2 * 100)
      }
      .writeAsCsv("country-attention.csv", WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Attention Analysis")
  }
}

