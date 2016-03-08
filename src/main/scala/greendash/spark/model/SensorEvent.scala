package greendash.spark.model

import play.api.libs.json.Json

case class SensorEvent ( tagName: String,
                         measurementType: String,
                         tagId: String,
                         tagType: String,
                         train: String,
                         processBlock: String,
                         timestamp: Long,
                         value: Double
                       )

object SensorEvent {
    implicit val f = Json.format[SensorEvent]
}

