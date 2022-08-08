import org.apache.spark.{SparkConf, SparkContext}
// It wont work spark1 version.
object MultipleSparkContext {

  def main(args:Array[String]): Unit = {
    val sparkConf= new SparkConf()
      .setMaster("local")
      .setAppName("MultipleSparkContext")

    val sc = new SparkContext(sparkConf)
    val sc1 = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array(1,2,3,4,5))
    val rdd1 = sc1.parallelize(Array(1,2,3,4,5))

    rdd.collect()
    rdd1.collect()

  }

}
