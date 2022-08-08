import org.apache.spark.sql.SparkSession

object FileFormat {

  def main(args:Array[String]) : Unit = {

    val sparkSession = SparkSession.builder()
      .appName("FileFormat")
      .master("local")
      .getOrCreate()

    val jsonDF = sparkSession.read.json("C:\\bigdata-testdata\\nameage.json")
    jsonDF.printSchema()
    jsonDF.show()
    println("count : " + jsonDF.count())


  }

}
