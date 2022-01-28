package process.datamigration

import common.utils.PropertiesUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import process.Consumer

/**
 * @author cys
 * @date 2022/1/28 14:43
 * @version 1.0
 * @Description
 */
class KafkaDataConsumer  extends Consumer{

  def getDataFromKafka(): Unit = {
    val kafkaConsumer = new KafkaConsumer[String, String](PropertiesUtil.getProperty("hbase_consumer.properties"))

  }

}
