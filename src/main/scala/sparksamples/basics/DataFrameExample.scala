
package sparksamples.basics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.{SQLContext}
import sparksamples.common.EnvironmentConstants


object DataFrameExample 
{ 
   def main(args: Array[String]) 
   { 
      val sc = new SparkContext( "local", "DataFrameExample",EnvironmentConstants.ScalaInstallationLocation, Nil, Map(), Map())
      val sqlContext = new SQLContext(sc)
      // this is used to implicitly convert an RDD to a DataFrame.
      val dfs = sqlContext.read.json(EnvironmentConstants.TestDataDirectoryRelativePath+"/employee.json")

      //see the data in the DataFrame
      /*val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/Temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      val dfs = spark.read.json(EnvironmentConstants.TestDataDirectoryRelativePath+"/employee.json")*/
      dfs.show()
   } 
} 