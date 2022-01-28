import dataproducer.DataProducer
import utils.KafkaUtil

import java.util

/**
 * @author cys
 * @date 2022/1/27 15:11
 * @version 1.0
 * @Description
 */
object Mian {

  var teleNumber = new util.ArrayList[String]
  var numberToName = new util.HashMap[String, String]
  val producer = new DataProducer

  def main(args: Array[String]): Unit = {
    producer.initMetadata(teleNumber, numberToName)
//    val result: String = producer.produce(teleNumber, numberToName, "2022-1-1", "2022-12-30")
//    producer.writeLog("data-producer/src/main/scala/Log/a.txt", result)
    KafkaUtil.writeDataIntoKafka()
  }

}
