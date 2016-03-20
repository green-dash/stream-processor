package greendash.spark

import greendash.spark.model.{MetricStats, SensorEvent, TagValues, TimedValue}
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
            .setMaster("local[2]")
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
        standardizeByTag(groupedByTagStream)

        // calculateSummaryStats(sensorStream)
        calculateDeviations(sensorStream)
    }

    def groupByTag(sensorStream: DStream[SensorEvent]) = {
        val tagStream: DStream[TagValues] = sensorStream
            .map(event => (event.tagName, (event.timestamp, event.value)))
            .groupByKeyAndWindow(streamGraphWindowSize, streamGraphSlideSize)
            .map { case (tag, list) =>
                val tvl = list map { case (t, v) => TimedValue(t, v) }
                TagValues(tag, tvl.toList)
            }

        val jsonStream = tagStream.map { _.toJson }

        val pStream = jsonStream
            .repartition(1) // needed for glomming (not best practice)
            .glom()
            .map(a => "[" + a.mkString(",") + "]")

        KafkaStreamPublisher.publishStream(groupedByTagTopic, pStream)

        tagStream
    }

    def standardizeByTag(groupedByTag: DStream[TagValues])= {
        val standardizedStream = groupedByTag.map { tv: TagValues =>
            val values = tv.values.map {_.value}
            val mean: Double = values.sum / values.length
            val devs = values.map(value => (value - mean) * (value - mean))
            val sd = Math.sqrt(devs.sum / values.length)
            val standardized = values.map(v => (v - mean) / sd)

            val zip = tv.values.map(_.timestamp).zip(standardized) map { case (t, v) => TimedValue(t,  if (v.isNaN || v.isInfinite) 0.0 else v)}
            TagValues(tv.tag, zip.toList)
        }

        val jsonStream = standardizedStream.map { _.toJson }

        val pStream = jsonStream
            .repartition(1) // needed for glomming (not best practice)
            .glom()
            .map(a => "[" + a.mkString(",") + "]")

        KafkaStreamPublisher.publishStream(standardizedByTagTopic, pStream)

        standardizedStream
    }

    def normalizeByTag(groupedByTag: DStream[TagValues])= {

        val normalizedStream = groupedByTag.map { tv: TagValues =>
            val values: List[Double] = tv.values.map {_.value}
            val max = values.max
            val min = values.min
            val d = max - min
            val normalized = values.map { v => if (d != 0.0) (v - min) / d else 1.0 }

            val zip = tv.values.map(_.timestamp).zip(normalized) map { case (t, v) => TimedValue(t, v)}
            TagValues(tv.tag, zip.toList)
        }

        val jsonStream = normalizedStream.map { _.toJson }

        val pStream = jsonStream
            .repartition(1) // needed for glomming (not best practice)
            .glom()
            .map(a => "[" + a.mkString(",") + "]")

        KafkaStreamPublisher.publishStream(normalizedByTagTopic, pStream)

        normalizedStream
    }


    def calculateSummaryStats(stream: DStream[SensorEvent]) = {

        val statStream = stream map { event =>
            (event.tagName, MetricStats(event.value))
        } reduceByKeyAndWindow(
            numPartitions = 1, // needed for glomming all keys into 1 stream (not for production !)
            reduceFunc = { (x, y) => x.merge(y) },
            windowDuration = summaryStatsWindowSize,
            slideDuration = summaryStatsSlideSize
        )

        val jsonStream = statStream map { case (tag, stats) =>
                s"""{ "tag": "$tag", "stats": ${stats.toJson} }"""
        }

        val pStream = jsonStream.glom().map(a => "[" + a.mkString(",") + "]")

        KafkaStreamPublisher.publishStream(summaryStatsTopic, pStream)

        statStream

    }

    def calculateDeviations(sensorStream: DStream[SensorEvent]) = {

        val statStream = sensorStream map { event =>
            (event.tagName, MetricStats(event.value))
        } reduceByKey(
            reduceFunc = { (x, y) => x.merge(y) }
        )

        val groupedStream = sensorStream map { event =>
            (event.tagName, event.value)
        }

        val joinedStream = groupedStream
            .join(statStream)
            .map { case (tag, (value, metricStats)) =>
                val xMean = if (value > metricStats.stats.mean) 1 else 0
                val xMean1s = if (value > metricStats.stats.mean + metricStats.stats.stdev) 1 else 0
                val xMean2s = if (value > metricStats.stats.mean + 2 * metricStats.stats.stdev) 1 else 0
                (tag, (xMean, xMean1s, xMean2s))
            }
            .reduceByKeyAndWindow(
                // numPartitions = 1, // needed for glomming all keys into 1 stream (not for production !)
                reduceFunc = (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3),
                windowDuration = summaryStatsWindowSize,
                slideDuration = summaryStatsSlideSize
            )

        val jsonStream = joinedStream map { case (tag, (xMean, xMean1s, xMean2s)) =>
                s"""{ "tag": "$tag", "deviations": [$xMean, $xMean1s, $xMean2s] }"""
        }

        val pStream = jsonStream
            .repartition(1) // needed for glomming (not best practice)
            .glom()
            .map(a => "[" + a.mkString(",") + "]")

        KafkaStreamPublisher.publishStream(deviationTopic, pStream)
    }

}
