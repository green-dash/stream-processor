package greendash.spark

import greendash.spark.model.{SensorEvent, TagValues, TimedValue}
import greendash.spark.pub.KafkaStreamPublisher
import greendash.spark.util.AppConfig._
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

        val groupedByTagStream = groupByTag(sensorStream)
        groupedByTagStream.cache()

        normalizeByTag(groupedByTagStream)
    }

    def groupByTag(sensorStream: DStream[SensorEvent]) = {
        val tagStream: DStream[TagValues] = sensorStream
            .map(event => (event.tagName, (event.timestamp, event.value)))
            .groupByKeyAndWindow(streamGraphWindowSize, streamGraphSlideSize)
            .map { case (tag, list) =>
                val tvl = list map { case (t, v) => TimedValue(t, v) }
                TagValues(tag, tvl.toList)
            }

        val pStream = tagStream.map { (tv: TagValues) => Json.stringify(Json.toJson(tv)) }

        KafkaStreamPublisher.publishStream(groupedByTagTopic, pStream)

        tagStream
    }

    def normalizeByTag(groupedByTag: DStream[TagValues])= {

        val normalizedStream = groupedByTag.map { tv: TagValues =>
            val valueList = tv.values.map {_.value}
            val max = valueList.max
            val min = valueList.min
            val d = max - min
            val normalized = valueList.map { v => if (d != 0.0) (v - min) / d else 1.0 }
            val zip = tv.values.map(_.timestamp).zip(normalized) map { case (t, v) => TimedValue(t, v)}
            TagValues(tv.tag, zip.toList)
        }

        val pStream = normalizedStream.map { (tv: TagValues) => Json.stringify(Json.toJson(tv)) }

        KafkaStreamPublisher.publishStream(normalizedByTagTopic, pStream)

        normalizedStream
    }

}
