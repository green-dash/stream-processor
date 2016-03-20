package greendash.spark.model

case class TimedValue(timestamp: Long, value: Double) {
    def toJson =  s"""{ "timestamp": $timestamp, "value": $value }"""
}

case class TagValues(tag:String, values: List[TimedValue]) {
    def toJson = {
        val jsonValues = "[" + values.map {_.toJson}.mkString(",") + "]"
        s"""{ "tag": "$tag", "values": $jsonValues }"""
    }
}

