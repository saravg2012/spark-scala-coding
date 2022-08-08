import org.apache.spark.sql.SparkSession

object ReadAvroFileFormat {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("ReadAvroFileFormat")
      .master("local")
      .getOrCreate()


    val avroDF = sparkSession.read.format("com.databricks.spark.avro").load("C:\\bigdata-testdata\\userdata1.avro")
    avroDF.printSchema()
    avroDF.show(5)
    println("Row Count " + avroDF.count())

  }
}
