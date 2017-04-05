
package sparksamples.basics

import org.apache.spark.SparkContext
import sparksamples.common.EnvironmentConstants

object SparkWordCount { 
   def main(args: Array[String])
   {
     //val sc = new SparkContext( "local", "Word Count",EnvironmentConstants.ScalaInstallationLocation, Nil, Map(), Map())
     val sc = new SparkContext( "local", "Word Count",System.getenv("SPARK_HOME"), Nil, Map(), Map())
      /* local = master URL; Word Count = application name; */
      /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */ 
      /* Map = variables to work nodes */ 
      /*creating an inputRDD to read text file (in.txt) through Spark context*/ 
      val input = sc.textFile(EnvironmentConstants.TestDataDirectoryRelativePath+"/sample.txt") 
      /* Transform the inputRDD into countRDD */ 
		
      val count = input.flatMap(line => line.split(" ")) 
      .map(word => (word, 1)) 
      .reduceByKey(_ + _) 
       
      /* saveAsTextFile method is an action that effects on the RDD */  
      count.saveAsTextFile("outfile") 
      System.out.println("OK"); 
   } 
} 