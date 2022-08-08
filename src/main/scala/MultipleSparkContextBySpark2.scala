import org.apache.spark.sql.SparkSession

object MultipleSparkContextBySpark2 {
  def main(args:Array[String]) : Unit = {
    val sparkSession1 = SparkSession.builder()
      .appName("MultipleSparkContextBySpark2")
      .master("local")
      .getOrCreate()

    val sparkSession2 = SparkSession.builder()
      .appName("MultipleSparkContextBySpark2")
      .master("local")
      .getOrCreate()

    val rdd1 = sparkSession1.sparkContext.parallelize(Array(1,2,3,4,5,6))
    val rdd2 = sparkSession2.sparkContext.parallelize(Array(7,7,8,9,0,1))
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)


  }

}
