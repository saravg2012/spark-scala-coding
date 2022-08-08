import org.apache.spark.sql.SparkSession

object DfOperationBasic {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("ReadAvroFileFormat")
      .master("local")
      .getOrCreate()

    val customerDF = sparkSession.read.orc("C:\\bigdata-testdata\\TestOrcFile.metaData.orc")
    val customerSchema = customerDF.schema
    println(customerSchema)

    val colNames = customerDF.columns
      println(colNames)
      println(colNames.mkString(", "))
   // val customerDescribeOption =  customerDF.describe("middle")
   // customerDescribeOption.show()

    val colAndTypes = customerDF.dtypes
    println("Column Types and Column dataType:")
    colAndTypes.foreach(println)

    customerDF.head(5).foreach(println)

  }
}
