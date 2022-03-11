package com.jaeyeong.datalake.datapipeline.bean

import java.util


/**
 * @Author: denggunghua
 * @Description:
 * @Date: 2022/03/11
 * E-mail:153447579@qq.com
 */
case class ResultBean(
                       database: String = null
,sqlList: util.List[String] = null
,table: String = null
,`type`: String = null



                          )
