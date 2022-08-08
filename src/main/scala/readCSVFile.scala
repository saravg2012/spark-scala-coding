import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object readCSVFile {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("readCSVFile").setMaster("local")
    val sc = new SparkContext(conf)

    val array = Array(1,2,3,4,5,6,7,8,9)
    val arrayRdd = sc.parallelize(array,2)

    println("Number of element : " + arrayRdd.count())
    arrayRdd.foreach(println)

    val file = "C:\\bigdata-testdata\\weatherHistory.csv"
    val fileRDD = sc.textFile(file,5)
    println("Number of rows in file : " + fileRDD.count())
    println(fileRDD.first())
    println(fileRDD.take(5))





  }
}


