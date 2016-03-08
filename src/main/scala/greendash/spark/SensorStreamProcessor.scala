package greendash.spark

import greendash.spark.model.{SensorEvent, TagValues}
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
            .map(event => (event.tagName, event.value))
            .groupByKeyAndWindow(streamGraphWindowSize, streamGraphSlideSize)
            .map { case (tag, list) =>
                val max = list.max
                val min = list.min
                val d = max - min
                val normalized = list.map { v => if (d != 0.0) (v - min) / d else v }
                TagValues(tag, normalized.toList)
            }
            .map { (tv: TagValues) => Json.stringify(Json.toJson(tv)) }

        printStream(graphStream)

        KafkaStreamPublisher.publishStream(streamGraphTopic, graphStream)
    }

}
