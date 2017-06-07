package com.yxtec.kafka

import com.google.gson.Gson
import ognl.Ognl
import ognl.OgnlException
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.*


data class Condition(
        /**
         * The Operators.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:09
         */
        var operators: String,

        /**
         * The Key.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:09
         */
        var key: String,

        /**
         * The Value.

         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:09
         */
        var value: Comparable<Any>) {


    /**
     * Check boolean.

     * @param dto the dto
     * *
     * @return the boolean
     * *
     * @throws OgnlException             the ognl exception
     * *
     * @throws InvocationTargetException the invocation target exception
     * *
     * @throws IllegalAccessException    the illegal access exception
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-06-05 21:30:09
     */
    @Throws(OgnlException::class, InvocationTargetException::class, IllegalAccessException::class)
    fun check(dto: Any): Boolean {
        val realValue: Comparable<Any>? = Ognl.getValue(key, dto) as Comparable<Any>
        val matched = if (realValue != null) {
            //=,>,<,>=,<=

            if (realValue.javaClass.name != value.javaClass.name) {
                val aClass = realValue.javaClass
                var valueOfMethod: Method? = null
                try {
                    valueOfMethod = aClass.getMethod("valueOf", value.javaClass)
                    value = valueOfMethod?.invoke(null, value) as Comparable<Any>
                } catch (e: Exception) {
                    println("参数${key}的值类型${value.javaClass.name}跟DTO中该字段的类型不匹配，且没找DTO该字段类型${aClass.name}的valueOf方法，此条件放弃!")
                }

                //
            }

            val compare = realValue.compareTo(value)
            var innerMatched = false;
            for (c in operators.toCharArray()) {
                when (c) {
                    '=' -> innerMatched = compare == 0
                    '>' -> innerMatched = compare > 0
                    '<' -> innerMatched = compare < 0
                    '@' -> innerMatched = realValue.toString().contains(value.toString())
                }
                if (innerMatched) {
                    break
                }
            }
            innerMatched
        } else {
            false
        }
        return matched
    }

    companion object {
        /**
         * Parse list.

         * @return the list
         * *
         * @throws IOException the io exception
         * *
         * @author :[刘杰](mailto:liujie@ebnew.com)
         * *
         * @date :2017-06-05 21:30:09
         */
        @Throws(IOException::class)
        internal fun parse(): List<Condition> {
            val conditions = ArrayList<Condition>()

            //搜索条件：
            var conf = File("condition.json")
            if (!conf.exists()) {
                conf = File(ClassLoader.getSystemResource("condition.json").file)
            }
            if (!conf.exists() || !conf.canRead()) {
                throw RuntimeException("搜索条件配置文件未找到,请检查condition.json文件是否存在!")
            }

            val conditionContent = FileUtils.readFileToString(conf, "UTF-8")
            val gson = Gson()
            val conditionMap = gson.fromJson<Map<String, Comparable<Any>>>(conditionContent, Map::class.java)
            for ((mKey, mValue) in conditionMap) {
                val operators = mKey.substringBefore("_")
                val key = mKey.substringAfter("_")

                val condition = Condition(operators, key, mValue)
                conditions.add(condition)
            }
            return conditions
        }
    }

}