package com.yxtec.kafka

/**
 * Kafka Broker对象，对应Kafka在zk上保存的broker信息
 *
 * @author :[刘杰](mailto:liujie@ebnew.com)
 *
 * @date :2017-06-05 21:30:09
 */
data class KafkaBroker(
        var host: String,
        var port: Int,
        var version: Int,
        var timestamp: Long,
        var jmxPort: Int
)