package com.jaeyeong.datalake.datapipeline.bean


/**
 * @Author: denggunghua
 * @Description:
 * @Date: 2022/03/11
 * E-mail:153447579@qq.com
 */
case class MysqlSBRBean(
                            data: String = null
,database: String = null
,es: Long = 0L
,id: Long = 0L
,isDdl: Boolean = false
,mysqlType: String = null
,old: String = null
,pkNames: String = null
,sql: String = null
,sqlType: String = null
,table: String = null
,ts: Long = 0L
,`type`: String = null


                          )
