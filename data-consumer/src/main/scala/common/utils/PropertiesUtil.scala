package common.utils

import java.io.{IOException, InputStream}
import java.util.Properties

/**
 * @author cys
 * @date 2022/1/28 14:20
 * @version 1.0
 * @Description
 */
object PropertiesUtil {

  var properties: Properties = null

  def getPropertyKey(propFileName: String,key: String) = {
    var properties: Properties = null

    val inputStream: InputStream = ClassLoader.getSystemResourceAsStream(propFileName)

    properties = new Properties()

    try {
      properties.load(inputStream)
    } catch {
      case e: IOException =>{e.printStackTrace()}
    }
    properties.getProperty(key)
  }

  def getProperty(propFileName: String) = {
    var properties: Properties = null

    val inputStream: InputStream = ClassLoader.getSystemResourceAsStream(propFileName)

    properties = new Properties()

    try {
      properties.load(inputStream)
    } catch {
      case e: IOException =>{e.printStackTrace()}
    }
    properties
  }


}
