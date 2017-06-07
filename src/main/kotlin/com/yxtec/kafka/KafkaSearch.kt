package com.yxtec.kafka

import com.google.gson.Gson
import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi.OffsetRequest
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.consumer.SimpleConsumer


class KafkaSearch {
    /**
     * The Gson.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    private val gson = Gson()

    /**
     * The M replica brokers.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    private val m_replicaBrokers = ArrayList<String>()

    /**
     * The Matched total.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    var matchedTotal: Long = 0

    /**
     * The Decoder.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    private val decoder = DtoDecoder()


    /**
     * Run.

     * @param config        the config
     * *
     * @param a_partition   the a partition
     * *
     * @param a_seedBrokers the a seed brokers
     * *
     * @param conditions    the conditions
     * *
     * @throws Exception the exception
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    @Throws(Exception::class)
    fun run(config: Config, a_partition: Int, a_seedBrokers: List<String>, conditions: List<Condition>) {
        // 获取指定Topic partition的元数据
        val metadata = findLeader(a_seedBrokers, config.topic, a_partition)
        if (metadata == null) {
            LOGGER.info("Can't find metadata for Topic and Partition. Exiting")
            return
        }
        val leader: Broker? = metadata.leader();
        leader?.let {
            var leadBroker = leader.host()
            val port = leader.port()
            val clientName = "Client_LiuJie_" + config.topic + "_" + a_partition

            val consumer: SimpleConsumer = SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName)
            var readOffset = config.getPosition(a_partition)
            if (readOffset == null || readOffset < 0) {
                readOffset = getLastOffset(consumer, config.topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName)
            }

            while (true) {
                val req = FetchRequestBuilder().clientId(clientName).addFetch(config.topic, a_partition, readOffset!!, 100000).build()
                val fetchResponse = consumer.fetch(req)

                if (fetchResponse.hasError()) {
                    // Something went wrong!
                    val code = fetchResponse.errorCode(config.topic, a_partition)
                    if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                        break
                    }
                    consumer.close()
                    leadBroker = findNewLeader(leadBroker, config.topic, a_partition, port)
                    continue
                }

                val numRead: Long = 0
                for (messageAndOffset in fetchResponse.messageSet(config.topic, a_partition)) {
                    val currentOffset: Long = messageAndOffset.offset()

                    if (readOffset != null) {
                        if (currentOffset < readOffset) {
                            currentOffset > 0
                            LOGGER.info("Found an old offset: {} Expecting: {}", currentOffset, readOffset)
                            continue
                        }
                    }

                    readOffset = messageAndOffset.nextOffset()
                    val payload = messageAndOffset.message().payload()

                    val bytes = ByteArray(payload.limit())
                    payload.get(bytes)

                    val dto = decoder.fromBytes(bytes)
                    var matched = true
                    if (conditions.isNotEmpty()) {
                        //OGNL处理
                        for (condition in conditions) {
                            val check = condition.check(dto)
                            if (!check) {
                                matched = false
                                break
                            }
                        }
                    }
                    if (matched) {
                        matchedTotal++
                        LOGGER.info("在分区{}位置{}处找到目标消息:{}", a_partition, currentOffset, gson.toJson(dto))
                    }
                    numRead.plus(1)
                }

                if (numRead == 0.toLong()) {
                    break
                }
                readOffset?.plus(1)
            }
            consumer.close()
        }
    }

    /**
     * Gets last offset.

     * @param consumer   the consumer
     * *
     * @param topic      the topic
     * *
     * @param partition  the partition
     * *
     * @param whichTime  the which time
     * *
     * @param clientName the client name
     * *
     * @return the last offset
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    fun getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientName: String): Long {
        val topicAndPartition = TopicAndPartition(topic, partition)
        val requestInfo = HashMap<TopicAndPartition, PartitionOffsetRequestInfo>()
        requestInfo.put(topicAndPartition, PartitionOffsetRequestInfo(whichTime, 1))
        val request = OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName)
        val response = consumer.getOffsetsBefore(request)

        if (response.hasError()) {
            LOGGER.error("Error fetching data Offset Data the Broker. Reason: {}", response.errorCode(topic, partition))
            return 0
        }
        val offsets = response.offsets(topic, partition)
        return offsets[0]
    }

    /**
     * Find new leader string.

     * @param a_oldLeader the a old leader
     * *
     * @param a_topic     the a topic
     * *
     * @param a_partition the a partition
     * *
     * @param a_port      the a port
     * *
     * @return String string
     * *
     * @throws Exception 找一个leader broker
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    @Throws(Exception::class)
    private fun findNewLeader(a_oldLeader: String, a_topic: String, a_partition: Int, a_port: Int): String {
        for (i in 0..2) {
            var goToSleep = false
            val metadata = findLeader(m_replicaBrokers, a_topic, a_partition)
            if (metadata == null) {
                goToSleep = true
            } else if (a_oldLeader.equals(metadata.leader().host(), ignoreCase = true) && i == 0) {
                // first time through if the leader hasn't changed give
                // ZooKeeper a second to recover
                // second time, assume the broker did recover before failover,
                // or it was a non-Broker issue
                //
                goToSleep = true
            } else {
                return metadata.leader().host()
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000)
                } catch (ie: InterruptedException) {
                }

            }
        }
        throw Exception("Unable to find new leader after Broker failure. Exiting")
    }

    /**
     * Find leader partition metadata.

     * @param a_seedBrokers the a seed brokers
     * *
     * @param a_topic       the a topic
     * *
     * @param a_partition   the a partition
     * *
     * @return the partition metadata
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:31:13
     */
    private fun findLeader(a_seedBrokers: List<String>, a_topic: String, a_partition: Int): PartitionMetadata? {
        var returnMetaData: PartitionMetadata? = null
        loop@ for (seed in a_seedBrokers) {
            var consumer: SimpleConsumer? = null
            try {
                val host = seed.substringBefore(":")
                val portStr = seed.substringAfter(seed, ":")
                val a_port = if (portStr.isNotBlank()) portStr.toInt() else 9092

                consumer = SimpleConsumer(host, a_port, 100000, 64 * 1024, "leaderLookup")
                val topics = listOf(a_topic)
                val req = TopicMetadataRequest(topics)
                val resp = consumer.send(req)

                val metaData = resp.topicsMetadata()
                for (item in metaData) {
                    for (part in item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part
                            break@loop
                        }
                    }
                }
            } catch (e: Exception) {
                LOGGER.error("Error communicating with Broker {} to find Leader for {}, {} Reason: $e", seed, a_topic, a_partition)
            } finally {
                if (consumer != null)
                    consumer.close()
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear()
            for (replica in returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host())
            }
        }
        return returnMetaData
    }
}