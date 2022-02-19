package com.jaeyeong.datalake.datapipeline.bean

import java.util

case class ResultBean(
                       database: String = null
,sqlList: util.List[String] = null
,table: String = null
,`type`: String = null



                          )
