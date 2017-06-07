package com.yxtec.kafka

import org.apache.commons.io.FileUtils
import org.apache.log4j.xml.DOMConfigurator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URL


class KafkaSearchApplication

val LOGGER: Logger = LoggerFactory.getLogger(KafkaSearchApplication::class.java)

fun main(args: Array<String>) {
    //Log4j
    var file = File("log4j.xml")
    if (!file.exists()) {
        file = File(ClassLoader.getSystemResource("log4j.xml").file)
    }
    DOMConfigurator.configureAndWatch(file.absolutePath, 100000)

    //kafka消费端配置
    val config = Config.parse()
    val seeds = config.brokers

    //解析条件
    val conditions = Condition.parse()

    //装载DTO Class
    val dtoFile = File("dto")
    if (dtoFile.exists() && dtoFile.isDirectory) {
        val dto = FileUtils.listFiles(dtoFile, arrayOf("jar"), false) as List<File>
        if (dto.isNotEmpty()) {
            val urls = ArrayList<URL>()
            dto.forEach {
                urls.add(it.toURI().toURL())
            }
            val dtoClassLoader = DtoClassLoader(urls.toTypedArray(), ClassLoader.getSystemClassLoader())
            Thread.currentThread().contextClassLoader = dtoClassLoader
        }
    }
    val kafkaSearch = KafkaSearch()
    LOGGER.info("搜索中 ...")
    try {
        for (i in 0..2) {
            kafkaSearch.run(config, i, seeds, conditions)
        }
        LOGGER.info("\r\n在kafka中搜索消息结束，共查询到${kafkaSearch.matchedTotal}条数据")
    } catch (e: Exception) {
        LOGGER.error(e.message, e)
    }

}


