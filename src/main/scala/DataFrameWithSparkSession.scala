import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameWithSparkSession {

  def main(args:Array[String]) : Unit = {

    val sparkSession = SparkSession.builder()
      .appName("DataFrameWithSparkSession")
      .master("local")
      .getOrCreate()

    val rdd = sparkSession.sparkContext.parallelize(Array(1,2,3,4,5,6))

    val schema = StructType(
      StructField("Integers as String", IntegerType,true):: Nil
    )

    val rowRdd = rdd.map(element => Row(element))
    val dataframe = sparkSession.createDataFrame(rowRdd,schema)

    dataframe.printSchema()
    dataframe.show(3)


  }

}
