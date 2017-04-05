package sparksamples.udf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import sparksamples.common.EnvironmentConstants

/**
  * Apache Spark UDAF definitions are currently supported in Scala and Java by the extending UserDefinedAggregateFunction class
  * 
  * User-defined aggregate functions (UDAFs) act on multiple rows at once, return a single value as a result, and typically work together with the GROUP BY statement
  * (for example COUNT or SUM).
  * We will implement a UDAF with alias SUMPRODUCT to calculate the retail value of all vehicles in stock grouped by make,
  * given a price and an integer quantity in stock in the following data:
  * {"Make":"Honda","Model":"Pilot","RetailValue":32145.0,"Stock":4}
    {"Make":"Honda","Model":"Civic","RetailValue":19575.0,"Stock":11}
    {"Make":"Honda","Model":"Ridgeline","RetailValue":42870.0,"Stock":2}
    {"Make":"Jeep","Model":"Cherokee","RetailValue":23595.0,"Stock":13}
    {"Make":"Jeep","Model":"Wrangler","RetailValue":27895.0,"Stock":4}
    {"Make":"Volkswagen","Model":"Passat","RetailValue":22440.0,"Stock":2}


  */
object ScalaUDAFExample {

  // Define the SparkSQL UDAF logic
  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    // Define the UDAF input and result schema's
    def inputSchema: StructType =     // Input  = (Double price, Long quantity)
      new StructType().add("price", DoubleType).add("quantity", LongType)
    def bufferSchema: StructType =    // Output = (Double total)
      new StructType().add("total", DoubleType)
    def dataType: DataType = DoubleType
    def deterministic: Boolean = true // true: our UDAF's output given an input is deterministic

    def initialize(buffer: MutableAggregationBuffer): Unit =
    {
      buffer.update(0, 0.0)           // Initialize the result to 0.0
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    {
      val sum   = buffer.getDouble(0) // Intermediate result to be updated
      val price = input.getDouble(0)  // First input parameter
      val qty   = input.getLong(1)    // Second input parameter
      buffer.update(0, sum + (price * qty))   // Update the intermediate result
    }
    // Merge intermediate result sums by adding them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
    {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }
    // THe final result will be contained in 'buffer'
    def evaluate(buffer: Row): Any =
    {
      buffer.getDouble(0)
    }
  }

  def main (args: Array[String])
  {
    val conf       = new SparkConf().setAppName("Scala UDAF Example") setMaster("local")
    val sc         = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val testDF = sqlContext.read.json(EnvironmentConstants.TestDataDirectoryRelativePath + "/inventory.json")
    testDF.registerTempTable("inventory")
    // Register the UDAF with our SQLContext
    sqlContext.udf.register("SUMPRODUCT", new SumProductAggregateFunction)

    sqlContext.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) AS InventoryValuePerMake FROM inventory GROUP BY Make").show()
  }
}