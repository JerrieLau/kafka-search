package com.yxtec.kafka

import java.io.IOException
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectStreamClass


class DtoObjectInputStream(input: InputStream) : ObjectInputStream(input) {

    /**
     * table mapping primitive type names to corresponding class objects
     */
    private val primClasses = mapOf(
            "boolean" to Boolean::class.java,
            "byte" to Byte::class.java,
            "char" to Char::class.java,
            "short" to Short::class.java,
            "int" to Integer::class.java,
            "long" to Long::class.java,
            "float" to Float::class.java,
            "double" to Double::class.java,
            "void" to Void::class.java
    )

    @Throws(IOException::class, ClassNotFoundException::class)
    override fun resolveClass(desc: ObjectStreamClass): Class<*> {
        val name = desc.name
        try {
            return Class.forName(name, false, Thread.currentThread().contextClassLoader)
        } catch (ex: ClassNotFoundException) {
            val cl = primClasses[name]
            if (cl != null) {
                return cl
            } else {
                throw ex
            }
        }

    }

}