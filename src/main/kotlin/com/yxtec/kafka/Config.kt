package com.yxtec.kafka

import com.google.gson.Gson
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException


data class Config(
        /**
         * The Brokers.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:27
         */
        var brokers: List<String>,

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
        var offsets: List<PartitionPosition>? = null
) {

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
             * The Partition.

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
        fun parse(): Config {
            var kafkaConfFile = File("kafka.json")
            if (!kafkaConfFile.exists()) {
                kafkaConfFile = File(ClassLoader.getSystemResource("kafka.json").file)
            }
            if (!kafkaConfFile.exists() || !kafkaConfFile.canRead()) {
                throw RuntimeException("kafka消费端配置文件未找到,请检查kafka.json文件是否存在!")
            }
            val kafkaContent = FileUtils.readFileToString(kafkaConfFile, "UTF-8")
            val gson = Gson()
            val kafkaConf = gson.fromJson<Config>(kafkaContent, Config::class.java)

            if (kafkaConf.topic.isEmpty()) {
                throw RuntimeException("未找到kafka消费端配置的topic，请检查topics配置项!")
            }

            // broker节点的ip
            if (kafkaConf.brokers.isEmpty()) {
                throw RuntimeException("未找到kafka消费端配置的brokers,请检查brokers配置项!")
            }
            return kafkaConf
        }
    }

}