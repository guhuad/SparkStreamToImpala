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

object OffsetUtil {


  //从数据库读取偏移量
  def getOffsetMap(groupid: String, topic: String) = {
  //  Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://172.17.20.121:3306/test?characterEncoding=UTF-8", "root", "875362")
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
    val connection = DriverManager.getConnection("jdbc:mysql://172.17.20.121:3306/test?characterEncoding=UTF-8", "root", "875362")
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

