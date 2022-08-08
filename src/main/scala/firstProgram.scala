import org.apache.spark.{SparkConf, SparkContext}

object firstProgram {
  def main(args :Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FirstProgram")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(5,10,3,10))
    println("Added number: "+ rdd.reduce(_+_))

  }

}
