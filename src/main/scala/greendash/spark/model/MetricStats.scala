package greendash.spark.model

import org.apache.spark.util.StatCounter

class MetricStats(private var stats: StatCounter) extends Serializable {

    def merge(other: MetricStats): MetricStats = {
        stats.merge(other.stats)
        this
    }

    def toJson =
        """{ "count":%d, "mean":%f, "stdev":%f, "max":%f, "min":%f }"""
            .format(stats.count, stats.mean, stats.stdev, stats.max, stats.min)

}

object MetricStats extends Serializable {

    def apply(x: Double) = new MetricStats(new StatCounter().merge(x))

}

