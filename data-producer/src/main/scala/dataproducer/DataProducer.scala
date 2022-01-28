package dataproducer

import java.io._
import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * @author cys
 * @date 2022/1/27 15:13
 * @version 1.0
 * @Description
 */
class DataProducer {

  def initMetadata(teleNumber: util.ArrayList[String], numberToName: util.HashMap[String, String]) = {
    teleNumber.add("17802596779")
    teleNumber.add("18907263863")
    teleNumber.add("19188980695")
    teleNumber.add("13320266126")
    teleNumber.add("19048828124")
    teleNumber.add("13653454072")
    teleNumber.add("13135279938")
    teleNumber.add("18281704261")
    teleNumber.add("17035534749")
    teleNumber.add("19834669675")
    teleNumber.add("19417467461")
    teleNumber.add("19772366326")
    teleNumber.add("18283449398")
    teleNumber.add("16005439091")
    teleNumber.add("14924565399")
    teleNumber.add("14218140347")
    teleNumber.add("17782151215")
    teleNumber.add("17340248510")
    teleNumber.add("19961057493")
    teleNumber.add("19724655139")
    numberToName.put("17802596779", "李雁")
    numberToName.put("18907263863", "卫艺")
    numberToName.put("19188980695", "仰莉")
    numberToName.put("13320266126", "陶欣悦")
    numberToName.put("19048828124", "施梅梅")
    numberToName.put("13653454072", "金虹霖")
    numberToName.put("13135279938", "魏明艳")
    numberToName.put("18281704261", "华贞")
    numberToName.put("17035534749", "华啟倩")
    numberToName.put("19834669675", "仲采绿")
    numberToName.put("19417467461", "卫丹")
    numberToName.put("19772366326", "戚丽红")
    numberToName.put("18283449398", "何翠柔")
    numberToName.put("16005439091", "钱溶艳")
    numberToName.put("14924565399", "钱琳")
    numberToName.put("14218140347", "缪静欣")
    numberToName.put("17782151215", "焦秋菊")
    numberToName.put("17340248510", "吕访琴")
    numberToName.put("19961057493", "沈丹")
    numberToName.put("19724655139", "褚美丽")
  }

  /**
   * 生产访问日志（Production access logs）
   * @param teleNumber 电话号码（phone number）
   * @param numberToName （电话号码，使用者），（phone number, user）
   * @param startTime （开始时间） （start time）
   * @param endTime （结束时间） （end time）
   * @return
   */
  def produce(teleNumber: util.ArrayList[String], numberToName: util.HashMap[String, String], startTime: String, endTime: String) ={
    initMetadata(teleNumber, numberToName)

    //获取随机数以获取随机电话号码(get a random number in order to get a random telephone number)
    val callerIndex = (Math.random() * teleNumber.size()).toInt

    val callerName: String = teleNumber.get(callerIndex)

    var calleeIndex = 0

    do {
      calleeIndex = (Math.random()*teleNumber.size()).toInt
    }
    while (callerIndex == calleeIndex)
    val calleeName: String = teleNumber.get(calleeIndex)


    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    var startDate: Date = null
    var endDate: Date = null

    startDate = sdf.parse(startTime)
    endDate = sdf.parse(endTime)

    val randomTs = startDate.getTime + (((endDate.getTime - startDate.getTime)) * Math.random()).toLong

    val resultDate = new Date(randomTs)
    val resultTimeStr: String = sdf.format(resultDate)

    val hour = (Math.random() * 24).toInt.toString
    val minute = (Math.random() * 60).toInt.toString
    val second = (Math.random() * 60).toInt.toString


    val specificTime: String = hour + ":" + minute + ":" + second

    val duration = (Math.random * 3600).toInt

    val result: String = callerName + " " + calleeName + " " + resultTimeStr + " " + specificTime + " " + duration

    result
  }

  def writeLog(filePath: String, result: String): Unit = {
//    var osw: OutputStreamWriter = null
//
//    osw= new OutputStreamWriter(new FileOutputStream(filePath, true), "utf-8")
//    osw.write(result)
//    osw.flush()
    var osw: OutputStreamWriter = null
    try { //you should use append not override,so you should use FileOutputStream(filePath,true),the true denote append
      osw = new OutputStreamWriter(new FileOutputStream(filePath, true), "utf-8")
      osw.write(result)
      osw.flush()
    } catch {
      case e: UnsupportedEncodingException =>
        e.printStackTrace()
      case e: FileNotFoundException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
  }


}
