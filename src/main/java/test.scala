package main.java

import java.util

import com.datastax.spark.connector._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by soumyaka on 11/7/2016.
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("IOT-test")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    //    val ssc = new StreamingContext(sc, Seconds(1))

    Logger.getRootLogger().setLevel(Level.ERROR)

    /*    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

        //topics is the set to which this Spark instance will listen.
        val topics = List("user-list-length").toSet

        val kafkaOutputBrokers = "DIN16000309:9092"
        val kafkaOutputTopic = "test-kafka"

        val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topics).map(_._2)

        lines.foreachRDD(rdd => {
          rdd.foreachPartition(partition => {

            val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
            partition.foreach(record => {
              val data = record.toString
              val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
              producer.send(message)

            })
            producer.close()
          })
        })*/

    val cassandraSQLRDD = sc.cassandraTable("iot", "userhistory")
    println(cassandraSQLRDD.count())
    val firstRow = cassandraSQLRDD.first()
    println(firstRow.toString())
    val userSpecificDataRDD = cassandraSQLRDD.where("user_id = ?", "0de52704-f425-4d4f-9500-b4fcaf7266ba")
      .where("date = ?", "2016-11-27")
    println(userSpecificDataRDD.count())
    userSpecificDataRDD.foreach(line => {
      val something = line.get[String]("user_id")
      println(something)
    })


    //    ssc.checkpoint("/Users/Ritabhari/IdeaProjects/IOT_Simulation_Spark_Cassandra-Integration/checkpoint/")
    //    ssc.start()
    //    ssc.awaitTermination()

  }

  def setupKafkaProducer(kafkaOutputBrokers: String): util.HashMap[String, Object] = {
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaOutputBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

}
