import org.apache.spark.sql.SparkSession


object createSparkContextWithSparkSession {

  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder()
      .appName("createSparkContextWithSparkSession")
      .master("local").getOrCreate()

    val array = Array(1,2,3,4,5,6,7,8)
    val arrayRDD =  sparkSession.sparkContext.parallelize(array,5)
    arrayRDD.foreach(println)

    val file = "C:\\bigdata-testdata\\mask_data.csv"
    val fileRDD = sparkSession.sparkContext.textFile(file,5)
    println("Number of rows in file : " + fileRDD.count())
    println(fileRDD.first())
    fileRDD.take(10).foreach(println)

    val header= fileRDD.first()
    val csvRDDWithOutHeader =  fileRDD.filter(_ != header)
    csvRDDWithOutHeader.take(10).foreach(println)
  // selected column is taken
    val csvArrayDateTime =  csvRDDWithOutHeader.map( line => {
      val colArray = line.split(",")
      (colArray(0),colArray(1),colArray(4))
    })
      .take(6).foreach(println)

    }

}
