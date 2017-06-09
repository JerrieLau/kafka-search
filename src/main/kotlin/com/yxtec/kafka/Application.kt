package com.yxtec.kafka

import org.apache.log4j.xml.DOMConfigurator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URL

/**
 * Kafka消息查询主类
 *
 * @author :[刘杰](mailto:liujie@ebnew.com)
 *
 * @date :2017-06-05 21:30:09
 */
class Application

val LOGGER: Logger = LoggerFactory.getLogger(Application::class.java)

fun main(args: Array<String>) {
    //Log4j
    var file = File("log4j.xml")
    if (!file.exists()) {
        file = File(ClassLoader.getSystemResource("log4j.xml").file)
    }
    DOMConfigurator.configureAndWatch(file.absolutePath, 100000)

    //kafka消费端配置
    val config = SearchConfig.parse()

    //解析条件
    val conditions = SearchCondition.parse()


    //装载DTO Class
    val dtoFile = File("dto")
    if (dtoFile.exists() && dtoFile.isDirectory) {
        val dto = dtoFile.listFiles().filter { it.name.endsWith(".jar") }
        if (dto.isNotEmpty()) {
            val urls = ArrayList<URL>()
            dto.forEach {
                urls.add(it.toURI().toURL())
            }
            val dtoClassLoader = DtoClassLoader(urls.toTypedArray(), ClassLoader.getSystemClassLoader())
            Thread.currentThread().contextClassLoader = dtoClassLoader
        }
    }

    LOGGER.info("搜索中 ...")
    try {
        var matchedTotal: Long = 0
        config.partitions.forEach {
            matchedTotal += it.search(config, conditions)
        }
        LOGGER.info("\r\n在kafka中搜索消息结束，共查询到${matchedTotal}条数据")
    } catch (e: Exception) {
        LOGGER.error(e.message, e)
    }

}


