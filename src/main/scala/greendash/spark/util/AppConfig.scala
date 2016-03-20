package greendash.spark.util

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.streaming.Milliseconds

case object AppConfig {

    val conf: Config = ConfigFactory.load()
    val checkpoint = conf.getString("spark.checkpoint")
    val batchSize = Milliseconds(conf.getLong("spark.batch.size"))

    /* kafka - consumer */
    val quorum = conf.getString("kafka.consumer.properties.quorum")
    val groupId = conf.getString("kafka.consumer.properties.group.id")
    val sensorTopic = conf.getString("kafka.consumer.topics.sensor")

    /* kafka - producer */
    val kafkaProducerProps = new Properties()
    kafkaProducerProps.put("bootstrap.servers", conf.getString("kafka.producer.properties.bootstrap.servers"))
    kafkaProducerProps.put("acks", conf.getString("kafka.producer.properties.acks"))
    kafkaProducerProps.put("key.serializer", conf.getString("kafka.producer.properties.key.serializer"))
    kafkaProducerProps.put("value.serializer", conf.getString("kafka.producer.properties.value.serializer"))

    /* stream graph settings */
    val normalizedByTagTopic = conf.getString("kafka.producer.topics.normalizedByTag")
    val standardizedByTagTopic = conf.getString("kafka.producer.topics.standardizedByTag")
    val groupedByTagTopic = conf.getString("kafka.producer.topics.groupedByTag")
    val streamGraphWindowSize = Milliseconds(conf.getLong("stream.graph.window.size"))
    val streamGraphSlideSize = Milliseconds(conf.getLong("stream.graph.slide.size"))

    /* summary statistics */
    val summaryStatsTopic = conf.getString("kafka.producer.topics.summaryStats")
    val deviationTopic = conf.getString("kafka.producer.topics.deviation")
    val summaryStatsWindowSize = Milliseconds(conf.getLong("summaryStats.window.size"))
    val summaryStatsSlideSize = Milliseconds(conf.getLong("summaryStats.slide.size"))

}
