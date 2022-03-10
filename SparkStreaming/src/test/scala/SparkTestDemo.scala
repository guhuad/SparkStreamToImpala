import java.util

import com.jaeyeong.datalake.datapipeline.bean.MysqlSBRBean

object SparkTestDemo extends App with Serializable {

  var mysqlBean: MysqlSBRBean =  new MysqlSBRBean

  val alterSqls = new util.ArrayList[String]
  val datebase = mysqlBean.data
  val table1 = mysqlBean.table
 // val db_tb = mysqlBean.table
  val db_tb = "po_header"
  val sqlStr = " ALTER TABLE  `" + "joymeo_test" + "`.`" + "ods_mysql_" + mysqlBean.database + "_" + mysqlBean.table + "_" + "a" + "` "
 // val alterSql_dec = mysqlBean.sql
 val alterSql_dec = "ALTER TABLE pub_order.po_header\n  ADD COLUMN first_camp_state TINYINT (1) DEFAULT 0 NOT NULL COMMENT '首营有状态，0-否，1-是',\n  ADD COLUMN id_card VARCHAR (18) DEFAULT '' NOT NULL COMMENT '身份证' ,\n  ADD COLUMN id_card_name VARCHAR (32) DEFAULT '' NOT NULL COMMENT '身份证姓名' ,\n  ADD COLUMN provide_invoic TINYINT (1) DEFAULT 0 NOT NULL COMMENT '是否开具发票，0-否，1-是' ,\n  ADD COLUMN prescription VARCHAR (200) DEFAULT '' NOT NULL COMMENT '处方单' ,\n  ADD COLUMN flag INT DEFAULT 0 NOT NULL COMMENT 'bd获取订单标识，0-未获取，1-已获取'"
  //修改 decimal(10, 2) 为 decimal(10,2),去掉空格干扰
  val alterSql0 = alterSql_dec.replaceAll("`", "")
  val alterDele_n = alterSql0.replaceAll("\n", " ")
  val alterDele_t = alterDele_n.replaceAll("\t", " ")
  val alterDele_r = alterDele_t.replaceAll("\r", " ")
  val alterDele_trim0 = alterDele_r.trim
  val alterDele_trim = alterDele_trim0.replaceAll("\\s+", " ")
  val alterSql1_1 = alterDele_trim.replaceAll(", ", ",")
  val alterSql1 = alterSql1_1.replaceAll(" ,", ",")
  val sql_sqlsplit = alterSql1.split(db_tb+" ")
  val alterStr = sql_sqlsplit(1)
  val alterSql6 =("\n"+alterStr).replaceAll(",","\n")
  val alterSql = alterSql6.toLowerCase
  //  System.out.println(alterSql)
  if (alterSql.contains("\n")) {
    var split = alterSql.split("\n")
    if (split.length > 1) for (i <- 1 until split.length) {
      val isDig = Character.isDigit(split(i).charAt(0))
      if(isDig){
        split(i-1) = split(i-1)+","+split(i)
        // val str: String = split.toBuffer.remove(i)
        //  val a = 0
      }
      //val   split1 = split
    }



    if (split.length > 1) for (i <- 1 until split.length) {
      if (split(i).contains("change column")) {
        val sb1 = new StringBuffer
        //  val alterSplit = split(i).toString
        //   alterSplit.deleteCharAt(sb2.lastIndexOf(",")).toString
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
      else if (split(i).contains("add column")) {
        val sb1 = new StringBuffer
        val s = split(i).split(" ")
        if (s.length > 3) {
          sb1.append(s(0) + " " + s(1) + " " + s(2) + " ")
          if (s(3).contains("int")) {
            sb1.append(" bigint ")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("decimal")) {
            sb1.append(s(3))
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("float")) {
            sb1.append(" float")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("double")) {
            sb1.append(" double")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else {
            sb1.append(" string")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          val a = 1
        }
      }else{
       // println("表结构异常修改："+ mysqlBean.toString)
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
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("decimal")) {
            sb1.append(s(3))
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("float")) {
            sb1.append(" float")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else if (s(3).contains("double")) {
            sb1.append(" double")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
            alterSqls.add(sqlStr + sb1.toString)
          }
          else {
            sb1.append(" string")
            for (i <- 3 until s.length) {
              if(s(i).contains("default")){
                sb1.append(" " + s(i) + " " + s(i+1))
              }
            }
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
          for (i <- 5 until splitLower.length) {
            if(splitLower(i).contains("default")){
              sb1.append(" " + splitLower(i) + " " + splitLower(i+1))
            }
          }
          alterSqls.add(sqlStr + sb1.toString)
        }
        else if (splitLower(6).contains("decimal")) {
          sb1.append(splitLower(6))
          for (i <- 5 until splitLower.length) {
            if(splitLower(i).contains("default")){
              sb1.append(" " + splitLower(i) + " " + splitLower(i+1))
            }
          }
          alterSqls.add(sqlStr + sb1.toString)
        }
        else if (splitLower(6).contains("float")) {
          sb1.append(" float")
          for (i <- 5 until splitLower.length) {
            if(splitLower(i).contains("default")){
              sb1.append(" " + splitLower(i) + " " + splitLower(i+1))
            }
          }
          alterSqls.add(sqlStr + sb1.toString)
        }
        else if (splitLower(6).contains("double")) {
          sb1.append(" double")
          for (i <- 5 until splitLower.length) {
            if(splitLower(i).contains("default")){
              sb1.append(" " + splitLower(i) + " " + splitLower(i+1))
            }
          }
          alterSqls.add(sqlStr + sb1.toString)
        }
        else {
          sb1.append(" string")
          for (i <- 5 until splitLower.length) {
            if(splitLower(i).contains("default")){
              sb1.append(" " + splitLower(i) + " " + splitLower(i+1))
            }
          }
          alterSqls.add(sqlStr + sb1.toString)
        }

        val a = 1
      }
    }else{
    //  println("表结构异常修改："+ mysqlBean.toString)
    }
  }
  alterSqls
  }

