package sparksamples.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sparksamples.common.EnvironmentConstants

object HelloSpark
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("HelloSpark").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    var sc =new SparkContext(conf)
    val inputFile =sc.textFile(EnvironmentConstants.TestDataDirectoryRelativePath+"/spam.txt")
    val nums =inputFile.map( x => x.split(" ").map(_.toDouble))
    val firstInfile =inputFile.first()
    val numsInfile =nums.first()
    println("firstInfile:"+firstInfile)
    println("numsInfile:"+numsInfile)
  }
}