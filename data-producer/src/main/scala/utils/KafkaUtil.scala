package utils

import dataproducer.DataProducer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util
import java.util.Properties

/**
 * @author cys
 * @date 2022/1/28 9:44
 * @version 1.0
 * @Description
 */
object KafkaUtil {

  private val dataProducer = new DataProducer
  private val teleNumber = new util.ArrayList[String]; //store telephone number;
  private val numberToName = new util.HashMap[String, String]


  def writeDataIntoKafka(): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.211.4:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    val stream: InputStream = utils.getPropUtil("kafka.properties")
//    properties.load(stream)

    val topic = "dbLog"
    val procuder = new KafkaProducer[String, String](properties)
    for (i <- 0 to 1000) {
      val value: String = dataProducer.produce(teleNumber, numberToName, "2022-1-29", "2022-12-29")

      try {
        Thread.sleep(2000)
      } //sleep 1000ms
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }

      val record = new ProducerRecord[String, String](topic, value)

      procuder.send(record)
    }
    println("end:write data")
    procuder.close()
  }


}
