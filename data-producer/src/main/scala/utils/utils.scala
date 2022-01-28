package utils

import java.io.InputStream
import java.util.Properties

/**
 * @author cys
 * @date 2022/1/28 10:45
 * @version 1.0
 * @Description
 */
object utils {
  //配置文件读取
  def getPropUtil(fileName: String) ={
    val properties = new Properties()
    val stream: InputStream = utils.getClass.getClassLoader.getResourceAsStream(fileName)
    stream
  }

}
