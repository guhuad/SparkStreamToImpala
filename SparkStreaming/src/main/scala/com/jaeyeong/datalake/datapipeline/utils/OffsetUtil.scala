package com.jaeyeong.datalake.datapipeline.utils

import java.sql.{Connection, DriverManager}
import java.util
import java.util.{ArrayList, List, Set}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * @Author: denggunghua
 * @Description:
 * @Date: 2022/03/11
 * E-mail:153447579@qq.com
 */
object OffsetUtil {


  val rcConf_active =  ConfigFactory.load("kafka")
  val rcConf = ConfigFactory.load(rcConf_active.getString("source.active"))

  //从数据库读取偏移量
  def getOffsetMap(groupid: String, topic: String) = {
  //  Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection(rcConf.getString("source.kafka.JDBC_CONN"), rcConf.getString("source.kafka.MYSQL_USER"), rcConf.getString("source.kafka.MYSQL_PASSWORD"))
    val pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic=?")
    pstmt.setString(1, groupid)
    pstmt.setString(2, topic)
    val rs = pstmt.executeQuery()
    val offsetMap = mutable.Map[TopicPartition, Long]()
    while (rs.next()) {
      offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
    }
    rs.close()
    pstmt.close()
    connection.close()
    offsetMap
  }

  //将偏移量保存到数据库
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
   // Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection(rcConf.getString("source.kafka.JDBC_CONN"), rcConf.getString("source.kafka.MYSQL_USER"), rcConf.getString("source.kafka.MYSQL_PASSWORD"))
    //replace into表示之前有就替换,没有就插入
    val pstmt = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
    for (o <- offsetRange) {
      pstmt.setString(1, o.topic)
      pstmt.setInt(2, o.partition)
      pstmt.setString(3, groupid)
      pstmt.setLong(4, o.untilOffset)
      pstmt.executeUpdate()
    }
    pstmt.close()
    connection.close()
  }


}

