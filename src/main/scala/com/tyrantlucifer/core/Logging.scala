package com.tyrantlucifer.core

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author chao.tian
 * @date 2022/6/14
 */
trait Logging {
  val log: Logger = LoggerFactory.getLogger(this.getClass)
}
