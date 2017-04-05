package sparksamples.udf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import sparksamples.common.EnvironmentConstants

/**
  * Created by Ayan on 4/5/2017.
  */
object UDFAndUDAF
{
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
    sparkConf.setAppName("UDFAndUDAF")

    val sc = new SparkContext(sparkConf)

    implicit val sqlContext = new SQLContext(sc)

    val addOne = (i: Int) => i + 1

    val df = sqlContext.read.json(EnvironmentConstants.TestDataDirectoryRelativePath + "/temperatures.json")
    println("Initial df " + df.show())
    df.registerTempTable("citytemps")
    // Register the UDF with our SQLContext

    sqlContext.udf.register("CTOF", (degreesCelcius: Double) => ((degreesCelcius * 9.0 / 5.0) + 32.0))
    sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
  }

  }
