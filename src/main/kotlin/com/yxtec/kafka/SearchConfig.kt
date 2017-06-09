package com.yxtec.kafka

import com.google.common.io.Files
import com.google.gson.Gson
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import java.io.File
import java.io.IOException
import java.io.InputStreamReader
import java.nio.charset.Charset

/**
 * 搜索配置
 *
 * @author :[刘杰](mailto:liujie@ebnew.com)
 *
 * @date :2017-06-05 21:30:09
 */
data class SearchConfig(

        var zkConnect: String?,

        /**
         * The Topic.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:27
         */
        var topic: String,

        /**
         * The Offsets.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:27
         */
        var offsets: List<PartitionPosition>?,

        /**
         * The Brokers.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:27
         */
        var brokers: MutableList<KafkaBroker>
) {

    lateinit var partitions: MutableList<KafkaPartition>

    /**
     * Gets position.

     * @param partition the partition
     * *
     * @return the position
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:30:27
     */
    fun getPosition(partition: Int): Long? {
        offsets?.forEach {
            if (it.partition == partition) {
                return it.offset
            }
        }
        return null
    }

    data class PartitionPosition(
            /**
             * The KafkaPartition.

             * @author :[刘杰](mailto:liujie@ebnew.com)
             * *
             * @date :2017-06-05 21:30:27
             */
            var partition: Int = 0,
            /**
             * The Offset.

             * @author :[刘杰](mailto:liujie@ebnew.com)
             * *
             * @date :2017-06-05 21:30:27
             */
            var offset: Long = 0
    )

    companion object {
        /**
         * Parse config.

         * @return the config
         * *
         * @throws IOException the io exception
         * *
         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:27
         */
        @Throws(IOException::class)
        fun parse(): SearchConfig {
            val kafkaConf: SearchConfig
            val kafkaConfFile = File("kafka.json")
            val gson = Gson()
            if (!kafkaConfFile.exists()) {
                val stream = ClassLoader.getSystemResourceAsStream("kafka.json")
                val reader = InputStreamReader(stream)
                kafkaConf = gson.fromJson<SearchConfig>(reader, SearchConfig::class.java)
                reader.close()
                stream.close()
            } else {
                val kafkaContent = Files.toString(kafkaConfFile, Charset.forName("UTF-8"))
                kafkaConf = gson.fromJson<SearchConfig>(kafkaContent, SearchConfig::class.java)
            }
//            kafkaConf ?: throw RuntimeException("kafka消费端配置文件未找到或不能读,请检查kafka.json文件是否存在!")

            //topic
            if (kafkaConf.topic.isEmpty()) {
                throw RuntimeException("未找到有效的topic配置，请检查topic配置项!")
            }

            kafkaConf.zkConnect?.isNotBlank().let {
                //解析该topic对应的分区
                val zooKeeper = ZooKeeper(kafkaConf.zkConnect, 600000, Watcher {
                })
                zooKeeper.let {
                    //获取partitions
                    var path = "/brokers/topics/${kafkaConf.topic}/partitions"
                    zooKeeper.exists(path, false) ?: throw IllegalArgumentException("从\"${kafkaConf.zkConnect}\"中查询Topic（${kafkaConf.topic}）的分区节点不存在，请检查Topic是否正确!")
                    val partitionList = zooKeeper.getChildren(path, false, null)
                    kafkaConf.partitions = mutableListOf<KafkaPartition>()
                    partitionList.forEach {
                        kafkaConf.partitions.add(KafkaPartition(it.toInt(), kafkaConf.topic))
                    }


                    //获取broker
                    path = "/brokers/ids"
                    zooKeeper.exists(path, false) ?: throw IllegalArgumentException("从\"${kafkaConf.zkConnect}\"上查询出brokers的实例节点不存在，请检查指定的zkConnect连接地址是否正确，zkConnect必须跟kafka配置文件server.xml中的zookeeper.connect一致!")

                    kafkaConf.brokers = mutableListOf()
                    val brokers: List<String> = zooKeeper.getChildren(path, false, null)
                    for (brokerId in brokers) {
                        //获取broker信息
                        val data = zooKeeper.getData("/brokers/ids/" + brokerId, false, null)

                        val brokerStr = data.toString(Charset.forName("UTF-8"))
                        val broker: KafkaBroker = gson.fromJson(brokerStr, KafkaBroker::class.java)

                        kafkaConf.brokers.add(broker)
                    }

                }
                return kafkaConf
            }
            throw RuntimeException("未找到有效的zkConnect配置,请检查zkConnect配置项!")
        }
    }

}