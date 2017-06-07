package com.yxtec.kafka

import java.net.URL
import java.net.URLClassLoader


class DtoClassLoader(urls: Array<URL>?, parent: ClassLoader?) : URLClassLoader(urls, parent)