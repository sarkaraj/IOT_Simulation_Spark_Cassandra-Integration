package main.java

import java.util

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by soumyaka on 11/7/2016.
  */
object IOTSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("IOT")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

    Logger.getRootLogger().setLevel(Level.ERROR)

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

    //topics is the set to which this Spark instance will listen.
    val topics = List("fitbit", "new-user-notification", "sales").toSet

    val kafkaOutputBrokers = "localhost:9092"
    val kafkaOutputTopic = "mapData"
    val keySpaceName = "iot"
    val tableName = "user_details"

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    val fitbitStream = lines.filter(_.split(",")(0) == "fitbit")

    warningNotification(fitbitStream, kafkaOutputTopic = "warningNotification", kafkaOutputBrokers)
    userHistory(fitbitStream, keySpaceName)
    val newUserStream = lines.filter(_.split(",")(0) == "new-user-notification")
      .map(line => {
        val array = line.split(",")
        val age = array(1).trim.toInt
        val gender = array(2).trim
        val category = array(3).trim
        val weight = array(4).trim.toDouble
        val height = array(5).trim.toDouble
        val bmi = array(6).trim.toDouble
        val bfp = array(7).trim.toDouble
        val bpCat = array(8).trim
        val bpSys = array(9).trim.toDouble
        val bpDia = array(10).trim.toDouble
        val userID = array(11).trim
        val deviceID = array(12).trim
        (userID, deviceID, age, bfp, bmi, bpCat, bpDia,
          bpSys, category, gender, height, weight)
        // updateUserTable(spark, updateRow)
      }).saveToCassandra(keySpaceName, tableName, SomeColumns("user_id", "device_id", "age", "bfp", "bmi", "bp_cat",
      "bp_dia", "bp_sys", "category", "gender", "height", "weight"))


    val saleStream = lines.filter(_.split(",")(0) == "sales")
      .map(line => {
        (line.split(",")(1).trim, line.split(",")(2).trim.toInt)
      })
      .saveToCassandra(keySpaceName, tableName = "sales", SomeColumns("date", "count"))


    userLatLongTable(fitbitStream, keySpaceName)

    //mapData(fitbitStream, kafkaOutputTopic, kafkaOutputBrokers)



    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  def warningNotification(fitbitStream: DStream[String], kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val data = fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val pulse = (array(5).trim.toDouble + 0.5).toInt
        val temp = array(6).trim.toDouble
        val age = array(7).trim.toInt
        val bpCat = array(8).trim
        val machineTimeStamp = array(9).trim

        val maxPulseLimit = {
          if (age < 40) 220 - age else 208 - 0.75 * age
        }

        val warning = {
          if (pulse >= 0.95 * maxPulseLimit) {
            if (List("HYP_1", "HYP_2", "HYP_CR").contains(bpCat)) "critical"
            else "simple"
          } else "no-use"
        }
        (userID, warning, machineTimeStamp)
      })
      .filter(_._2 != "no-use")


    data.foreachRDD(rdd => {
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
  }

  def userHistory(fitbitStream: DStream[String], keySpaceName: String): Unit = {
    fitbitStream
      .map(line => {
        val array = line.split(",")
        val simulationTime = array(1).trim
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        (simulationTime, userID, lat, long)
      }).foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        partition.foreach(record => {
          println(record.toString)
        })
      })
    })
  }

  def userLatLongTable(fitbitStream: DStream[String], keySpaceName: String): Unit = {
    fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        (userID, lat, long)
      }).saveToCassandra(keySpaceName, tableName = "latest_location", SomeColumns("user_id", "lat", "long"))
  }

  def mapData(fitbitStream: DStream[String], kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val data = fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        val pulse = (array(5).trim.toDouble + 0.5).toInt
        val temp = array(6).trim.toDouble
        (userID, lat, long, pulse, temp)
      })

    data.foreachRDD(rdd => {
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
