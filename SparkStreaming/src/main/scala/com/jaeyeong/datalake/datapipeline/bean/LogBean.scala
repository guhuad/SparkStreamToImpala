package com.jaeyeong.datalake.datapipeline.bean

import java.util

case class LogBean(
                    account: String = null
                    ,appId: String = null
                    ,appVersion: String = null
                    ,carrier: String = null
                    ,deviceId: String = null
                    ,deviceType: String = null
                    ,ip: String = null
                    ,latitude: Double = 0.0
                    ,longitude: Double = 0.0
                    ,netType: String = null
                    ,osName: String = null
                    ,osVersion: String = null
                    ,releaseChannel: String = null
                    ,resolution: String = null
                    ,sessionId: String = null
                    ,timeStamp: Long = 0L
                    ,eventId: String = null
                    ,properties: util.Map[String, String] = null
                          )
