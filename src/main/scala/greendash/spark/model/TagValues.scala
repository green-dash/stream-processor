package greendash.spark.model

import play.api.libs.json.Json

case class TagValues(tag:String, values: List[Double])
object TagValues {
    implicit val f = Json.format[TagValues]
}

