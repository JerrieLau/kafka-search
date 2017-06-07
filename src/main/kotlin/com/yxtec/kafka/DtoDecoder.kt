package com.yxtec.kafka

import java.io.ByteArrayInputStream


class DtoDecoder : kafka.serializer.Decoder<Any> {

    override fun fromBytes(bytes: ByteArray): Any {
        var input: DtoObjectInputStream? = null
        try {
            val bais = ByteArrayInputStream(bytes)
            input = DtoObjectInputStream(bais)
            return input.readObject()
        } finally {
            input?.close()
        }
    }

}