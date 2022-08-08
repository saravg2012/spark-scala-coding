import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object kafkaSparkDemo {

  def main(args: Array[String]) : Unit = {

    val broker_id = "localhost:9092"
    val groupid="GRP1"
    val topics = "testtopic"

    val topicset = topics.split(",").toSet
    val kafkaParams = Map[String,Object] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf().setMaster("local").setAppName("Kafka Demo")
    val ssc = new StreamingContext(sparkconf,Seconds(5))
    val sc =  ssc.sparkContext
    sc.setLogLevel("OFF")
    val message = KafkaUtils.createDirectStream[String,String] (
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicset,kafkaParams)
    )

    val words = message.map(_.value()).flatMap(_.split(" "))
    val countWords = words.map(x => (x,1)).reduceByKey(_+_)
    countWords.print()
    ssc.start()
    ssc.awaitTermination()



  }

}
