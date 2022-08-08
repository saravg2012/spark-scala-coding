import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.::

object ImposingOwnSchema {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ReadAvroFileFormat")
      .master("local")
      .getOrCreate()

    val nameDF = sparkSession.read
      .option("header",true)
      .option("inferSchema","true")
      .csv("C:\\bigdata-testdata\\trans.csv")

       println("inferSchema")
       nameDF.printSchema()

    val ownSchema = StructType(
        StructField("CustomerId",StringType,true) ::
        StructField("CustomerName",StringType,true) ::
        StructField("dateTime",LongType,true) ::
        StructField("discount",StringType,true) ::
        StructField("Member",StringType,true) :: Nil
    )

    val namesWithOwnSchema = sparkSession.read
      .option("header","true")
      .schema(ownSchema)
      .csv("C:\\bigdata-testdata\\trans.csv")

      println("Custom schema imposed with StructType")
      namesWithOwnSchema.printSchema()

    val customerSchema = namesWithOwnSchema.schema
    println(customerSchema)

  }

}
