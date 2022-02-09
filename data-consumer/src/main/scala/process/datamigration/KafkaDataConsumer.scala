package process.datamigration

import common.utils.PropertiesUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import process.Consumer

import java.util

/**
 * @author cys
 * @date 2022/1/28 14:43
 * @version 1.0
 * @Description
 */
class KafkaDataConsumer  extends Consumer{

  def getDataFromKafka(): Unit = {

    val kafkaConsumer =
      new KafkaConsumer[String, String](PropertiesUtil.properties)

    kafkaConsumer.subscribe(util.Arrays.asList(PropertiesUtil.getPropertyKey("hbase_consumer.properties","kafka.topics")))






  }

}
