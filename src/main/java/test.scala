package main.java

import java.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by soumyaka on 11/7/2016.
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.set("spark.cassandra.connection.host", "DIN16000309")
      .setAppName("IOT-test")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

    Logger.getRootLogger().setLevel(Level.ERROR)

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

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
    })


    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

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
