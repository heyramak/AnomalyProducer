package io.heyram.producer

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util._
import java.time

import com.google.gson.{JsonObject, Gson}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import java.nio.charset.Charset;
import org.apache.kafka.clients.producer._



object TrasactionProducer {

  var applicationConf:Config = _
  val props = new Properties()
  var topic:String =  _
  var producer:KafkaProducer[String, String] = _

  def load = {

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("kafka.bootstrap.servers"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.key.serializer"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("kafka.value.serializer"))
    props.put(ProducerConfig.ACKS_CONFIG, applicationConf.getString("kafka.acks"))
    props.put(ProducerConfig.RETRIES_CONFIG, applicationConf.getString("kafka.retries"))
    topic = applicationConf.getString("kafka.topic")
  }

  def getCsvIterator(fileName:String) = {

    val file = new File(fileName)
    val csvParser = CSVParser.parse(file, Charset.forName("UTF-8"), CSVFormat.DEFAULT)
    csvParser.iterator()
  }



  def publishJsonMsg(fileName:String) = {
    val gson: Gson = new Gson
    val csvIterator = getCsvIterator(fileName)
    val rand: Random = new Random
    var count = 0

    while (csvIterator.hasNext) {
      val record = csvIterator.next()

      val obj: JsonObject = new JsonObject
      val ts: String = time.LocalDateTime.now().toString



      //println("Transaction Details:" + record.get(0),record.get(1),record.get(2),record.get(3),timestamp, record.get(7),record.get(8),record.get(9), record.get(10), record.get(11))
      obj.addProperty(TransactionKafkaEnum.trans_time, ts)
      obj.addProperty(TransactionKafkaEnum.duration, record.get(0))
      obj.addProperty(TransactionKafkaEnum.protocol_type, record.get(1))
      obj.addProperty(TransactionKafkaEnum.service, record.get(2))
      obj.addProperty(TransactionKafkaEnum.flag, record.get(3))
      obj.addProperty(TransactionKafkaEnum.src_bytes, record.get(4))
      obj.addProperty(TransactionKafkaEnum.dst_bytes, record.get(5))
      obj.addProperty(TransactionKafkaEnum.land, record.get(6))
      obj.addProperty(TransactionKafkaEnum.wrong_fragment, record.get(7))
      obj.addProperty(TransactionKafkaEnum.urgent, record.get(8))
      obj.addProperty(TransactionKafkaEnum.hot, record.get(9))
      obj.addProperty(TransactionKafkaEnum.num_failed_logins, record.get(10))
      obj.addProperty(TransactionKafkaEnum.logged_in, record.get(11))
      obj.addProperty(TransactionKafkaEnum.num_compromised, record.get(12))
      obj.addProperty(TransactionKafkaEnum.root_shell, record.get(13))
      obj.addProperty(TransactionKafkaEnum.su_attempted, record.get(14))
      obj.addProperty(TransactionKafkaEnum.num_root, record.get(15))
      obj.addProperty(TransactionKafkaEnum.num_file_creations, record.get(16))
      obj.addProperty(TransactionKafkaEnum.num_shells, record.get(17))
      obj.addProperty(TransactionKafkaEnum.num_access_files, record.get(18))
      obj.addProperty(TransactionKafkaEnum.num_outbound_cmds, record.get(19))
      obj.addProperty(TransactionKafkaEnum.is_host_login, record.get(20))
      obj.addProperty(TransactionKafkaEnum.is_guest_login, record.get(21))
      obj.addProperty(TransactionKafkaEnum.count, record.get(22))
      obj.addProperty(TransactionKafkaEnum.srv_count, record.get(23))
      obj.addProperty(TransactionKafkaEnum.serror_rate, record.get(24))
      obj.addProperty(TransactionKafkaEnum.srv_serror_rate, record.get(25))
      obj.addProperty(TransactionKafkaEnum.rerror_rate, record.get(26))
      obj.addProperty(TransactionKafkaEnum.srv_rerror_rate, record.get(27))
      obj.addProperty(TransactionKafkaEnum.same_srv_rate, record.get(28))
      obj.addProperty(TransactionKafkaEnum.diff_srv_rate, record.get(29))
      obj.addProperty(TransactionKafkaEnum.srv_diff_host_rate, record.get(30))
      obj.addProperty(TransactionKafkaEnum.dst_host_count, record.get(31))
      obj.addProperty(TransactionKafkaEnum.dst_host_srv_count, record.get(32))
      obj.addProperty(TransactionKafkaEnum.dst_host_same_srv_rate, record.get(33))
      obj.addProperty(TransactionKafkaEnum.dst_host_diff_srv_rate, record.get(34))
      obj.addProperty(TransactionKafkaEnum.dst_host_same_src_port_rate, record.get(35))
      obj.addProperty(TransactionKafkaEnum.dst_host_srv_diff_host_rate, record.get(36))
      obj.addProperty(TransactionKafkaEnum.dst_host_serror_rate, record.get(37))
      obj.addProperty(TransactionKafkaEnum.dst_host_srv_serror_rate, record.get(38))
      obj.addProperty(TransactionKafkaEnum.dst_host_rerror_rate, record.get(39))
      obj.addProperty(TransactionKafkaEnum.dst_host_srv_rerror_rate, record.get(40))


      val json: String = gson.toJson(obj)
      println("Transaction Record: " + json)
      val producerRecord = new ProducerRecord[String, String](topic, json) //Round Robin Partitioner
      //val producerRecord = new ProducerRecord[String, String](topic, json.hashCode.toString, json)  //Hash Partitioner
      //val producerRecord = new ProducerRecord[String, String](topic, 1, json.hashCode.toString, json)  //Specific Partition
      //producer.send(producerRecord) //Fire and Forget
      //producer.send(producerRecord).get() /*Synchronous Producer */
      producer.send(producerRecord, new MyProducerCallback) /*Asynchrounous Produer */
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
    }
  }

  class MyProducerCallback extends Callback {
    def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
      if (e != null) System.out.println("AsynchronousProducer failed with an exception" + e)
      else {
        System.out.println("Sent data to partition: " + recordMetadata.partition + " and offset: " + recordMetadata.offset)
      }
    }
  }

  def main(args: Array[String]) {

    applicationConf = ConfigFactory.parseFile(new File(args(0)))
    load
    producer = new KafkaProducer[String, String](props)
    val file = applicationConf.getString("kafka.producer.file")
    publishJsonMsg(file)


  }
}
