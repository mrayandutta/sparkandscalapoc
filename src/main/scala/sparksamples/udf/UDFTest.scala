package sparksamples.udf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import sparksamples.common.EnvironmentConstants
import org.apache.log4j._

/**
  * Created by Ayan on 4/5/2017.
  */

object UDFTest {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
    sparkConf.setAppName("UDFTest")

    val sc = new SparkContext(sparkConf)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val addOne = (i: Int) => i + 1

    val df = sqlContext.read.json(EnvironmentConstants.TestDataDirectoryRelativePath+"/people.json")
    println("Initial df "+df.show())

    /*
    Example 1 : The example below wraps simple Scala function addOne as Spark UDF via call to higher order function org.apache.spark.sql.functions.udf.
    You can only use the returned function via DSL API.
     */
    // Works only with DSL API
    val addOneByFunction = udf(addOne)

    //DSL
    df.select(addOneByFunction($"age") as "test1D").show

    /*
    Example 2 : The example below wraps simple Scala function as Spark UDF via call to higher order function sqlContext.udf.register.
    You can use both registered function (with SQL) and returned function (with DSL).
     */

    // Works With Sql Expr and DSL API
    val addOneByRegister = sqlContext.udf.register("addOneByRegister", addOne)

    //SQL
    df.selectExpr("addOneByRegister(age) as test2S").show

    //DSL
    df.select(addOneByRegister($"age") as "test2D").show

    //DSL by callUDF
    df.select(callUDF("addOneByRegister", $"age") as "test2CallUDF").show

    // How to pass literal values to UDF ?
    /*
    Example  3 : The example below wraps simple Scala function literal which takes two parameters as input and returns the sum of the two parameters
    as Spark UDF via call to higher order function org.apache.sql.functions.udf. You can only use the returned function via DSL API.
    We will pass the first parameter as literal value via lit function in org.apache.spark.sql.functions package
    We will pass second parameter age column from DataFrame.
     */

    val addByLit = udf((i: Int, x: Int) => x + i)

    //DSL `lit` function
    df.select(addByLit(lit(3), $"age") as "testLitF").show
    /*
    Example  4 :The example below wraps simple Scala function addByCurryFunction as Spark UDF via call to higher order function org.apache.sql.functions.udf.
    You can only use the returned function via DSL API.
    */
    def addByCurryFunction(i: Int) = udf((x: Int) => x + i)

    //DSL literal value via CURRYING
    df.select(addByCurryFunction(2)($"age") as "testLitC").show

    /*
    Example  5 : The example below wraps simple Scala function addByCurry as Spark UDF via call to higher order function sqlContext.udf.register.
    You can use both registered function (with SQL) and returned function (with DSL).
     */

    def addByCurry(i: Int) = (x: Int) => x + i

    val addByCurryRegister = sqlContext.udf.register("addByCurryRegister", addByCurry(2))

    //SQL literal value via CURRYING
    df.selectExpr("addByCurryRegister(age) as testLitC1").show

    //DSL literal value via CURRYING
    df.select(addByCurryRegister($"age") as "testLitC2").show

    //More than 22 UDF Argument
    val udfWith23Arg = udf((array: Seq[Double]) => array.sum)

    //DSL Example
    df.select(udfWith23Arg(array($"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal",
      $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal",
      $"sal", $"sal", $"sal", $"sal", $"sal", $"sal", $"sal")) as "total").show

    sc.stop()
  }

}
