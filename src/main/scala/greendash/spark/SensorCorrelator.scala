package greendash.spark

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object SensorCorrelator {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("SensorCorrelator")
            // .set("spark.logConf", "true")
            // .set("spark.akka.logLifecycleEvents", "true")

        val sc = new SparkContext(sparkConf)

        import org.apache.spark.mllib.linalg._
        import org.apache.spark.mllib.stat.Statistics
        val csvRdd = sc.textFile("/Users/koen/projects/green-dash/data/all.csv").map(_.split(","))
        val doubleRdd = csvRdd.map { a => a.map(_.toDouble) }
        val vectorRdd = doubleRdd.map(Vectors.dense)
        val corr = Statistics.corr(vectorRdd)

        val rows = corr.toString(100000, 100000).split("\n")
        rows.foreach { row =>
            println (row.replaceAll("\\s+", ",").replaceAll(",$", ""))
        }

    }

    def toTag(fname: String) = {
        fname.replaceAll(".*/", "").replaceAll("\\.csv", "")
    }

    def fileList = {
        val dir = ConfigFactory.load().getString("data.folder")
        val d = new File(dir)
        d.listFiles.filter(_.isFile).map(_.getCanonicalPath).toList
    }

}
