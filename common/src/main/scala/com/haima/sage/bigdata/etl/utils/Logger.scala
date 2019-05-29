package com.haima.sage.bigdata.etl.utils

import org.slf4j.LoggerFactory

/**
  * Created by zhhuiyan on 16/4/15.
  */

trait Logger {

  protected def loggerName = this.getClass.getCanonicalName

  lazy val logger = LoggerFactory.getLogger(loggerName)
}

/**
  * Helper to create stand-alone loggers with fixed names.
  */
object Logger {
  private[Logger] val CLASSPATH = classOf[Logger].getClass.getResource("/").getPath


  def init(logFile: String): Unit = {
    if (logFile != null && new java.io.File(CLASSPATH + "/" + logFile).exists()) {
      import ch.qos.logback.classic.LoggerContext
      import ch.qos.logback.classic.joran.JoranConfigurator
      import ch.qos.logback.core.joran.spi.JoranException
      import ch.qos.logback.core.util.StatusPrinter
      import org.slf4j.LoggerFactory

      val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

      try {
        val configurator = new JoranConfigurator
        configurator.setContext(context)
        // Call context.reset() to clear any previous configuration, e.g. default
        // configuration. For multi-step configuration, omit calling context.reset().
        context.reset()
        configurator.doConfigure(CLASSPATH + "/" + logFile)
      } catch {
        case je: JoranException =>

      }
      StatusPrinter.printInCaseOfErrorsOrWarnings(context)
    }

  }


  def apply[T](clazz: Class[T]): Logger = new Logger {
    override val loggerName = clazz.getCanonicalName
  }

  def apply(name: String): Logger = new Logger {
    override val loggerName = name
  }
}
