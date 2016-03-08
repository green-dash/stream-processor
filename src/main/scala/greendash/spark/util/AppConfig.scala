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
    val streamGraphTopic = conf.getString("kafka.producer.topics.stream.graph")
    val streamGraphWindowSize = Milliseconds(conf.getLong("stream.graph.window.size"))
    val streamGraphSlideSize = Milliseconds(conf.getLong("stream.graph.slide.size"))

}
