package greendash.spark.model

import play.api.libs.json.Json

case class TimedValue(timestamp: Long, value: Double)
object TimedValue {
    implicit val f = Json.format[TimedValue]
}

case class TagValues(tag:String, values: List[TimedValue])
object TagValues {
    implicit val f = Json.format[TagValues]
}

