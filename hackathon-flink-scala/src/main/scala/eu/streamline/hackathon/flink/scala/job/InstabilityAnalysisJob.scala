package eu.streamline.hackathon.flink.scala.job

import java.time.Instant

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

object InstabilityAnalysisJob {

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

    val w = TumblingEventTimeWindows.of(Time.days(1))

    val conflictEvents = gdeltEventStream
      .filterWith{ case (ev, count) =>
        // material conflict quadclass 4
        // protest: eventcode 14
        (ev.quadClass != null & ev.quadClass == 4) |
          (ev.eventRootCode != null & ev.eventRootCode.equals("14"))
      }
      .windowAll(w)
      .apply {
        (window, events, out: Collector[(Instant, Long)]) =>
          out.collect((Instant.ofEpochMilli(window.getStart), events.map(_._2).sum))
      }

    val eventsAll = gdeltEventStream
      .windowAll(w)
      .apply { (window, events, out: Collector[(Instant, Long)]) =>
        out.collect((Instant.ofEpochMilli(window.getStart), events.map(_._2).sum))
      }

    // Number of conflict events every day as percentage of all events
    conflictEvents.join(eventsAll).where(_._1).equalTo(_._1).window(w)
      .apply { (c, all) =>
        (c._1, c._2.toDouble / all._2 * 100)
      }
      .writeAsCsv("global-instability.csv", WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Global Instability Analysis")
  }
}

