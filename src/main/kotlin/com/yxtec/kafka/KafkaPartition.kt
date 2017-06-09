package com.yxtec.kafka

import com.google.gson.Gson
import kafka.api.FetchRequestBuilder
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.consumer.SimpleConsumer
import java.lang.IllegalArgumentException
import java.util.*


/**
 * Kafka分区
 *
 * @author :[刘杰](mailto:liujie@ebnew.com)
 *
 * @date :2017-06-05 21:30:09
 */
data class KafkaPartition(
        var id: Int,
        var topic: String
) {

    companion object {
        val gson = Gson()
        val decoder = DtoDecoder()
    }

    fun findLeaderBroker(brokers: List<KafkaBroker>): kafka.cluster.Broker? {
        brokers.forEach {
            val consumer = SimpleConsumer(it.host, it.port, 100000, 64 * 1024, "leaderLookup")
            val topics = listOf(topic)
            val req = TopicMetadataRequest(topics)
            val resp = consumer.send(req)

            resp.topicsMetadata()
                    .flatMap { it.partitionsMetadata() }
                    .filter { it.partitionId() == id }
                    .forEach { return it.leader() }
        }
        return null
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
        val request = kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName)
        val response = consumer.getOffsetsBefore(request)

        if (response.hasError()) {
            throw IllegalArgumentException("从Broker中获取${topic}主题尚存的最早位置异常，原因: ${response.errorCode(topic, partition)}")
        }
        val offsets = response.offsets(topic, partition)
        return offsets[0]
    }


    fun search(config: SearchConfig, conditions: List<SearchCondition>): Long {
        var matchedTotal: Long = 0

        var leader = findLeaderBroker(config.brokers)
        leader ?: throw IllegalArgumentException("查找${id}分区的主分区所在的Broker为空")

        val clientName = "Client_${config.topic}_${id}_${UUID.randomUUID()}"
        var readOffset = config.getPosition(id)
        var consumer: SimpleConsumer? = null
        do {
            consumer = consumer ?: SimpleConsumer(leader!!.host(), leader.port(), 100000, 64 * 1024, clientName)
            if (readOffset == null || readOffset < 0) {
                readOffset = getLastOffset(consumer, config.topic, id, kafka.api.OffsetRequest.EarliestTime(), clientName)
            }
            val req = FetchRequestBuilder().clientId(clientName).addFetch(topic, id, readOffset, 100000).build()
            val fetchResponse = consumer.fetch(req)

            if (fetchResponse.hasError()) {
                // Something went wrong!
                val code = fetchResponse.errorCode(config.topic, id)
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    break
                }
                consumer.close()
                consumer = null
                leader = findLeaderBroker(config.brokers)
                continue
            }

            val numRead: Long = 0
            for (messageAndOffset in fetchResponse.messageSet(config.topic, id)) {
                val currentOffset: Long = messageAndOffset.offset()

                if (readOffset != null) {
                    if (currentOffset < readOffset) {
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
                    LOGGER.info("在分区{}位置{}处找到目标消息:{}", id, currentOffset, gson.toJson(dto))
                }
                numRead.plus(1)
            }

            if (numRead == 0.toLong()) {
                break
            }
            readOffset?.plus(1)
        } while (true)

        consumer?.close()

        return matchedTotal
    }

}