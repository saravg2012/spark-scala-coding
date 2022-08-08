
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object countword {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local")
    val sc = new SparkContext(conf)

   // val lines = sc.textFile("src/main/wordCountSample.txt")
   val lines = sc.textFile("C:/kafka_2.11-2.3.0/wordCountSample.txt")
    val words = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts)
      println(word + " : " + count)
  }


}
