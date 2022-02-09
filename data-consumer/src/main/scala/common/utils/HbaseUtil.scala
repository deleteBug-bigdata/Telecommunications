package common.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}

import java.text.DecimalFormat
import java.util

/**
 * @author cys
 * @date 2022/1/28 15:26
 * @version 1.0
 * @Description
 */
object HbaseUtil {

  /**
   * 判断表是否存在
   * judge the tableName if not exist
   * @param conf
   * @param tableName
   * @return
   */
  def isExistTable(conf: Configuration, tableName: String) = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    val result: Boolean = admin.tableExists(TableName.valueOf(tableName))
    admin.close()
    conn.close()
    result
  }

  /**
   * 判断表空间是否存在
   * judge the tableSpace if not exist
   * @param conf
   * @param tableSpace
   */
  def isExistSpace(conf: Configuration, tableSpace: String) = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    val result = false
    result
  }

  /**
   * 初始化命名空间
   * Initialize namespace
   * @param conf
   * @param namespace
   */
  def initNamespace(conf: Configuration, namespace: String)={
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    val nd: NamespaceDescriptor = NamespaceDescriptor.create(namespace)
      .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
      .addConfiguration("AUTHOR", "Lawson")
      .build()

    admin.createNamespace(nd)
    admin.close()
    conn.close()
  }

  def genSplitKeys(regions: Int) = {
    /**
     * 定义一个存放分区键的数组
     * Define an array to hold the partition key
     */
    val keys = new Array[String](regions)

    val df = new DecimalFormat("00")

    for (i <- 0 until regions ) {
      keys(i) = df.format(i) + "|"
    }

    val splitKeys = new Array[Array[Byte]](regions)

    val treeSet = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)

    for (i <- 0 until regions ) {
      treeSet.add(Bytes.toBytes(keys(i)))
    }

    val splitKeysIterator: util.Iterator[Array[Byte]] = treeSet.iterator()
    var index = 0
    while (splitKeysIterator.hasNext) {
      val b: Array[Byte] = splitKeysIterator.next()

      splitKeys({
        index += 1; index - 1
      }) = b
    }
    splitKeys

  }

  /**
   * 创建表：协处理器
   * Create Table: Coprocessors
   *
   * @param conf
   * @param tableName
   * @param regions
   * @param columnFamily
   */
  def createTable(conf: Configuration, tableName: String, regions: Int, columnFamily: String*): Unit = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin

    if (isExistTable(conf, tableName)) {
      return
    }

    val htd = new HTableDescriptor(TableName.valueOf(tableName))
    for (cf <- columnFamily) {
      htd.addFamily(new HColumnDescriptor(cf))
    }
    //向该表添加一个表协处理器。 htd.addCoprocessor("hbase.CalleeWriteObserver");
    val temp: Array[Array[Byte]] = genSplitKeys(regions)

    admin.createTable(htd, genSplitKeys(regions))
    admin.close();
    conn.close()
  }

  /**
   * 生成行键
   * generate a rowkey
   * regionCode_call1_buildTime_call2_flag_duration
   *
   * @param regionCode
   * @param call1
   * @param buildTime
   * @param call2
   * @param flag
   * @param duration
   * @return
   */
  def genRowKey(regionCode: String, call1: String, buildTime: String, call2: String, flag: String, duration: String)= {
    val sb = new StringBuilder
    sb.append(regionCode + "_").append(call1 + "_").append(buildTime + "_")
      .append(call2 + "_").append(flag + "_").append(duration);
    sb.toString()
  }

  /**
   * 1.您应该在同一年月份保持相同的数字存储在同一个分区中
   * 2.因此 regionCode 取决于 call1（这是一个数字），buildTime（它们是调用构建的年份和月份），
   *
   * 1.you should keep same number in same year month are stored in same partition
   * 2.so the regionCode are depended on call1(this is a number),buildTime(they are year and month that call were build),
   *
   * @param call1
   * @param buildTime
   * @param regions
   */
  def genRegionCode(call1: String, buildTime: String, regions: Int)= {
    val len: Int = call1.length

    //获得最后一个四分位数
    //get last quartile in number
    val lastPhone: String = call1.substring(len - 4)

    //格式化并获取年份和月份
    //format and get year and month
    val ym: String = buildTime.replaceAll("-", "").replaceAll(":", "")
      .replaceAll(" ", "").substring(0, 6)

    val x: Int = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym)
    var a = 10
    var b = 20
    a = a ^ b
    b = a ^ b
    a = a ^ b

    //second scatter operation
    val y: Int = x.hashCode()

    val regionCode: Int = y % regions

    val df = new DecimalFormat("00")
    df.format(regionCode)
  }


}
