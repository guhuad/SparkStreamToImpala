package com.jaeyeong.datalake.datapipeline.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util
import java.util.{ArrayList, List, Set}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.jaeyeong.datalake.datapipeline.bean.{MysqlSBRBean, ResultBean}

object SqlContants {

  private val DATABASE_STR = "joymeo_dev"

  private val JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver"

  // private static String CONNECTION_URL="jdbc:impala://node1:21050/default;auth=noSasl";
  private val CONNECTION_URL = "jdbc:impala://172.17.20.122:21050/"

 // private val DATABASE_STR = "joymeo_data"


  //定义数据库连接

  var conn:Connection = null

  //定义PreparedStatement对象

   var ps:PreparedStatement = null

  //定义查询的结果集

  var rs:ResultSet = null
  /**
   * 拼接 UPDATE
   *
   * @param mysqlBean
   * @return
   */
  def getUpdateSql(mysqlBean: MysqlSBRBean): util.List[String] = {
    /**
     * UPDATE c_test.t_order1 SET order_id = 'testupdate', amount = 101 WHERE id = 14;
     */
    //        //String sqlStr= "create table if not exists "+DATABASE_STR+"."+"ods_mysql_"+mysqlBean.getDatabase()+"_"+mysqlBean.getTable()+"_"+"a"+"( \n";
    val updateSqls = new util.ArrayList[String]
    val sqlStr = " update  `" + DATABASE_STR + "`.`" + "ods_mysql_" + mysqlBean.database + "_" + mysqlBean.table + "_" + "a" + "` set "
    val dataObjects = JSON.parseArray(mysqlBean.data)
    val jsonObjectList = new util.ArrayList[JSONObject]
    var jsonObjectData = JSON.parseObject("")
    val jsonObjectType = JSON.parseObject(mysqlBean.mysqlType)
    val jsonObjectOld = JSON.parseArray(mysqlBean.old)
    //拼接 where 条件
    val pks = JSON.parseArray(mysqlBean.pkNames)
    for (j <- 0 until dataObjects.size) {
      val sb1 = new StringBuffer
      val sb2 = new StringBuffer
      val dataStr = dataObjects.get(j).toString
      jsonObjectData = JSON.parseObject(dataStr)
      val dataSets = jsonObjectData.keySet
      val oldStr = jsonObjectOld.get(j).toString
      val jsonOldObject = JSON.parseObject(oldStr)
      val oldKeySets = jsonOldObject.keySet
      val oldObjects = oldKeySets.toArray
      if (dataObjects.size > 1) {
        val a = 0
      }
      //  for (int i = 0; i < oldKeySets.size(); i++) {
      import scala.collection.JavaConversions._
      for (dataSet <- dataSets) {
        if (!pks.contains(dataSet)) if (jsonObjectType.getString(dataSet).toLowerCase.contains("int") || jsonObjectType.getString(dataSet).toLowerCase.contains("decimal") || jsonObjectType.getString(dataSet).toLowerCase.contains("float") || jsonObjectType.getString(dataSet).toLowerCase.contains("double")) sb1.append("`" + dataSet + "`" + " = " + jsonObjectData.getString(dataSet) + ",")
        else sb1.append("`" + dataSet + "`" + " = " + "'" + jsonObjectData.getString(dataSet) + "'" + ",")
      }
      //  }
      //去掉最后的【,】
      sb1.deleteCharAt(sb1.lastIndexOf(",")).toString
      //拼接 where ,
      sb1.append(" where ")
      import scala.collection.JavaConversions._
      for (pk <- pks) { // sb1.append(pk.toString()+",");
        if (jsonObjectType.getString(pk.toString).toLowerCase.contains("int") || jsonObjectType.getString(pk.toString).toLowerCase.contains("decimal") || jsonObjectType.getString(pk.toString).toLowerCase.contains("float") || jsonObjectType.getString(pk.toString).toLowerCase.contains("double")) sb2.append("`" + pk.toString + "`" + " = " + jsonObjectData.getString(pk.toString) + ",")
        else sb2.append("`" + pk.toString + "`" + " = " + "'" + jsonObjectData.getString(pk.toString) + "'" + ",")
      }
      // 掉条件 where 【,】
      sb2.deleteCharAt(sb2.lastIndexOf(",")).toString
      updateSqls.add(sqlStr + sb1.toString + sb2.toString)
    }
    if (updateSqls.size > 1) {
      val a = 0
    }
    updateSqls
  }


  /**
   * upsert into,存在则更新，不存在则插入
   *
   * @param mysqlBean
   * @return
   */
  def getUpsertIntoSql(mysqlBean: MysqlSBRBean): util.List[String] = {
    val insertSql = new util.ArrayList[String]
    //String sqlStr= "create table if not exists "+DATABASE_STR+"."+"ods_mysql_"+mysqlBean.getDatabase()+"_"+mysqlBean.getTable()+"_"+"a"+"( \n";
    val sqlStr = " upsert into `" + DATABASE_STR + "`.`" + "ods_mysql_" + mysqlBean.database + "_" + mysqlBean.table + "_" + "a" + "` "
    val sb1 = new StringBuffer
    val sb2 = new StringBuffer
    // Object toJSON = JSON.toJSON(mysqlBean.getData());
    //  JSONObject jsonObject1 = JSON.parseObject(parse.toString());
    //  //json对象转Map
    //  Map<String,Object> map = (Map<String,Object>)jsonObject1;
    // Collection<Object> values = map.values();
    // JSONObject jsonObject1 = JSON.(mysqlBean.getData());
    // JSONObject jsonObject = JSON.parseObject(mysqlBean.getData());
    //   //jsonObject.
    val objects1 = JSON.parseArray(mysqlBean.data)
    val jsonObject = JSON.parseObject(mysqlBean.mysqlType)
    import scala.collection.JavaConversions._
    for (o <- objects1) {
      val s = o.toString
      val jsonObject1 = JSON.parseObject(s)
      val strings = jsonObject.keySet
      // Collection<Object> values = jsonObject1.values();
      val objects = strings.toArray
      for (i <- 0 until strings.size) {
        sb1.append("`" + objects(i).toString + "`" + ",")
        if (jsonObject.getString(objects(i).toString).toLowerCase.contains("int") || jsonObject.getString(objects(i).toString).toLowerCase.contains("decimal") || jsonObject.getString(objects(i).toString).toLowerCase.contains("float") || jsonObject.getString(objects(i).toString).toLowerCase.contains("double")) sb2.append(jsonObject1.getString(objects(i).toString) + ",")
        else {
          sb2.append("'" + jsonObject1.getString(objects(i).toString) + "'" + ",")
          //  int a = 1;
        }
      }
      //   for (String string : strings) {
      //       String string1 = jsonObject.getString(string.toString());
      //       sb1.append(string+",");
      //   }
      //
      //   for (Object value : values) {
      //       sb2.append("'"+value.toString()+"'"+",");
    }
    //      for (Object o : jsonObject) {
    //          //json对象转Map
    //          Map<String,Object> map = (Map<String,Object>)o;
    //          Collection<Object> values = map.values();
    //          int s;
    //      }
    //     int size = objects.size();
    //     for (Object object : objects) {
    //         String s = object.toString();
    //         sb.append(object.toString()+",");
    //     }
    val colStr = "(" + sb1.deleteCharAt(sb1.lastIndexOf(",")).toString + ")"
    val valueStr = " values (" + sb2.deleteCharAt(sb2.lastIndexOf(",")).toString + ")"
    // String s = sb.deleteCharAt(sb.lastIndexOf(",")).toString();
    // sqlStr+sb.toString(
    // for (Map.Entry<String, String> entry : entries) {
    //     //"and properties['pageId'] = 'page006'"
    //     sb.append("\n and properties['"+entry.getKey()+"']='"+entry.getValue()+"'");
    // }
    insertSql.add(sqlStr + colStr + valueStr)
    insertSql
  }


  // mysql数据同步Sql
  @throws[SQLException]
  def sqlExecute(rsBean: ResultBean): Unit = {
     conn = getConn(CONNECTION_URL + DATABASE_STR)
    val sqlList = rsBean.sqlList
    //   conn=getConn(CONNECTION_URL);
    // ps = conn.prepareStatement(rsBean.getSql());
    //  ps.executeQuery();
    val sqlStr = ""
    try if (sqlList != null) {
      import scala.collection.JavaConversions._
      for (sql <- sqlList) { // System.out.println(sql);
       ps = conn.prepareStatement(sql)
        ps.execute
      }
    }
    catch {
      case e: SQLException =>

      //   System.out.println(sqlStr);
     //  e.printStackTrace();
    } finally {
      if (ps != null) try //关闭
        ps.close()
      catch {
        case e: SQLException =>

         // e.printStackTrace();
      }
      if (conn != null) try conn.close()
      catch {
        case e: SQLException =>

      }
    }
  }


  /**
   * 拼接 DELETE
   *
   * @param mysqlBean
   * @return
   */
  def getDeleteSql(mysqlBean: MysqlSBRBean): util.List[String] = {
    /**
     * DELETE FROM joymeo_data.ods_mysql_bd_customer_payment_reporting_a  WHERE id = '1456114920622829570'
     */
    //        //String sqlStr= "create table if not exists "+DATABASE_STR+"."+"ods_mysql_"+mysqlBean.getDatabase()+"_"+mysqlBean.getTable()+"_"+"a"+"( \n";
    val updateSqls = new util.ArrayList[String]
    val sqlStr = " DELETE FROM  `" + DATABASE_STR + "`.`" + "ods_mysql_" + mysqlBean.database + "_" + mysqlBean.table + "_" + "a" + "` "
    val dataObjects = JSON.parseArray(mysqlBean.data)
    val jsonObjectList = new util.ArrayList[JSONObject]
    var jsonObjectData = JSON.parseObject("")
    val jsonObjectType = JSON.parseObject(mysqlBean.mysqlType)
    val jsonObjectOld = JSON.parseArray(mysqlBean.old)
    //拼接 where 条件
    val pks = JSON.parseArray(mysqlBean.pkNames)
    for (j <- 0 until dataObjects.size) {
      val sb1 = new StringBuffer
      val sb2 = new StringBuffer
      val dataStr = dataObjects.get(j).toString
      jsonObjectData = JSON.parseObject(dataStr)
      //拼接 where ,
      sb1.append(" where ")
      import scala.collection.JavaConversions._
      for (pk <- pks) { // sb1.append(pk.toString()+",");
        if (jsonObjectType.getString(pk.toString).toLowerCase.contains("int") || jsonObjectType.getString(pk.toString).toLowerCase.contains("decimal") || jsonObjectType.getString(pk.toString).toLowerCase.contains("float") || jsonObjectType.getString(pk.toString).toLowerCase.contains("double")) sb2.append("`" + pk.toString + "`" + " = " + jsonObjectData.getString(pk.toString) + ",")
        else sb2.append("`" + pk.toString + "`" + " = " + "'" + jsonObjectData.getString(pk.toString) + "'" + ",")
      }
      // 掉条件 where 【,】
      sb2.deleteCharAt(sb2.lastIndexOf(",")).toString
      updateSqls.add(sqlStr + sb1.toString + sb2.toString)
    }
    updateSqls
  }

  //数据库连接
  def getConn(cu: String): Connection = {
    try {
      Class.forName(JDBC_DRIVER)
      conn = DriverManager.getConnection(cu)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    conn
  }


  /**
   * 拼接 ALTER
   *
   * @param mysqlBean
   * @return
   */
  def getAlterSql(mysqlBean: MysqlSBRBean): util.List[String] = {
    /**
     * ALTER TABLE `c_test`.`t_order1` add COLUMN `create_time` string
     * String sqlStr= "create table if not exists "+DATABASE_STR+"."+"ods_mysql_"+mysqlBean.getDatabase()+"_"+mysqlBean.getTable()+"_"+"a"+"( \n";
     * 只做增加字段修改
     */
    val alterSqls = new util.ArrayList[String]
    val sqlStr = " ALTER TABLE  `" + DATABASE_STR + "`.`" + "ods_mysql_" + mysqlBean.database + "_" + mysqlBean.table + "_" + "a" + "` "
    val alterSql_dec = mysqlBean.sql
    //修改 decimal(10, 2) 为 decimal(10,2),去掉空格干扰
    val alterSql = alterSql_dec.replaceAll(", ", ",")
  //  System.out.println(alterSql)
    if (alterSql.contains("\n")) {
      val split = alterSql.toLowerCase.split("\n")
      if (split.length > 1) for (i <- 1 until split.length) {
        if (split(1).contains("change column")) {
          val sb1 = new StringBuffer
          val s = split(i).split(" ")
          if (s.length > 4) {
            sb1.append(s(0) + " " + s(1) + " " + s(2) + " " + s(3) + " ")
            if (s(4).contains("int")) {
              sb1.append(" bigint ")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(4).contains("decimal")) {
              sb1.append(s(4))
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(4).contains("float")) {
              sb1.append(" float")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(4).contains("double")) {
              sb1.append(" double")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else {
              sb1.append(" string")
              alterSqls.add(sqlStr + sb1.toString)
            }
          }
          val a = 1
        }
        else if (split(1).contains("add column")) {
          val sb1 = new StringBuffer
          val s = split(i).split(" ")
          if (s.length > 3) {
            sb1.append(s(0) + " " + s(1) + " " + s(2) + " ")
            if (s(3).contains("int")) {
              sb1.append(" bigint ")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("decimal")) {
              sb1.append(s(3))
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("float")) {
              sb1.append(" float")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("double")) {
              sb1.append(" double")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else {
              sb1.append(" string")
              alterSqls.add(sqlStr + sb1.toString)
            }
            val a = 1
          }
        }
      }
      else if (split.length > 0) for (i <- 1 until split.length) {
        if (split(1).contains("add column")) {
          val sb1 = new StringBuffer
          val s = split(i).split(" ")
          if (s.length > 3) {
            sb1.append(s(0) + " " + s(1) + " " + s(2) + " ")
            if (s(3).contains("int")) {
              sb1.append(" bigint ")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("decimal")) {
              sb1.append(s(3))
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("float")) {
              sb1.append(" float")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else if (s(3).contains("double")) {
              sb1.append(" double")
              alterSqls.add(sqlStr + sb1.toString)
            }
            else {
              sb1.append(" string")
              alterSqls.add(sqlStr + sb1.toString)
            }
            val a = 1
          }
        }
      }
    }
    else {
      val sqlLower = alterSql.toLowerCase
      val splitLower = sqlLower.split(" ")
      if (splitLower.length > 1 && sqlLower.contains("change column")) {
        val sb1 = new StringBuffer
        // String[] s = split[i].split(" ");
        if (splitLower.length > 6) {
          sb1.append(" change column " + splitLower(5) + " " + splitLower(6) + " ")
          if (splitLower(7).contains("int")) {
            sb1.append(" bigint ")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(7).contains("decimal")) {
            sb1.append(splitLower(7))
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(7).contains("float")) {
            sb1.append(" float")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(7).contains("double")) {
            sb1.append(" double")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else {
            sb1.append(" string")
            alterSqls.add(sqlStr + sb1.toString)
          }
        }
        val a = 1
      }
      else if (splitLower.length > 0 && sqlLower.contains("add column")) {
        val sb1 = new StringBuffer
        if (splitLower.length > 5) {
          sb1.append(" add column " + " " + splitLower(5) + " ")
          if (splitLower(6).contains("int")) {
            sb1.append(" bigint ")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(6).contains("decimal")) {
            sb1.append(splitLower(6))
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(6).contains("float")) {
            sb1.append(" float")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (splitLower(6).contains("double")) {
            sb1.append(" double")
            alterSqls.add(sqlStr + sb1.toString)
          }
          else {
            sb1.append(" string")
            alterSqls.add(sqlStr + sb1.toString)
          }
          val a = 1
        }
      }
    }
    alterSqls
  }

}



