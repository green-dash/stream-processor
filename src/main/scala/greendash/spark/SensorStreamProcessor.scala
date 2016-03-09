package greendash.spark

import greendash.spark.model.{SensorEvent, TagValues, TimedValue}
import greendash.spark.pub.KafkaStreamPublisher
import greendash.spark.util.AppConfig._
import greendash.spark.util.AppUtil._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json.Json

object SensorStreamProcessor {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("SensorStreamProcessor")
            .set("spark.logConf", "true")
            .set("spark.akka.logLifecycleEvents", "true")

        val ssc = new StreamingContext(sparkConf, batchSize)
        ssc.checkpoint(checkpoint)

        process(ssc)

        ssc.start()
        ssc.awaitTermination()
    }

    def process(ssc: StreamingContext) = {

        /* capture the stream and transform into sensor events */
        val sensorStream: DStream[SensorEvent] = KafkaUtils
            .createStream(ssc, quorum, groupId, Map(sensorTopic -> 1))
            .map(_._2)
            .map(Json.parse)
            .flatMap(_.asOpt[SensorEvent])

        sensorStream.cache()

        publishGraphStream(ssc, sensorStream)
    }

    def publishGraphStream(ssc: StreamingContext, sensorStream: DStream[SensorEvent]) = {

        val graphStream = sensorStream
            .map(event => (event.tagName, (event.timestamp, event.value)))
            .groupByKeyAndWindow(streamGraphWindowSize, streamGraphSlideSize)
            .map { case (tag, list) =>
                val valueList = list.map {_._2}
                val max = valueList.max
                val min = valueList.min
                val d = max - min
                val normalized = valueList.map { v => if (d != 0.0) (v - min) / d else 1.0 }
                val zip = list.map(_._1).zip(normalized) map { case (t, v) => TimedValue(t, v)}
                TagValues(tag, zip.toList)
            }
            .map { (tv: TagValues) => Json.stringify(Json.toJson(tv)) }

        printStream(graphStream)

        KafkaStreamPublisher.publishStream(streamGraphTopic, graphStream)
    }

}
