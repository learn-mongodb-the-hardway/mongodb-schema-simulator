package com.mtools.schemasimulator.cli

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.core.Appender
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.FilterReply
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.HelpFormatter
import com.xenomachina.argparser.ShowHelpException
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.slf4j.Marker
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.Writer
import java.net.URI
import java.util.*

object App : KLogging() {
    private val props by lazy {
        Properties().apply {
            load(Config::class.java.classLoader.getResourceAsStream("config.properties"))
        }
    }

    val name by lazy {
        props["name"]?.toString() ?: "unknown"
    }

    val gitRev by lazy {
        props["gitRev"]?.toString() ?: "unknown"
    }
    val version by lazy {
        props["version"]?.toString() ?: "unknown"
    }

    fun execute(args: Array<String>) = execute {
        if (args.isEmpty()) {
            loadConfig(arrayOf("--help"))
        }

        logger.info("version      : ${App.version}")
        logger.info("git revision : ${App.gitRev}")

        // Parse the options
        val config = loadConfig(args)
        // Attempt to validate the options
        config.validate()

        // The input stream
        val stream = BufferedInputStream(File(config.general.config!!).inputStream())

        if (config.general.worker) {
            WorkerExecutor(WorkerExecutorConfig(
                URI("http://${config.general.masterURI}"),
                URI("http://${config.general.host}:${config.general.port}")
            ))
        } else {
            MasterExecutor(MasterExecutorConfig(
                master=config.general.master,
                uri=URI("http://${config.general.host}:${config.general.port}"),
                config=InputStreamReader(stream).readText(),
                graphOutputDPI = config.general.dpi,
                graphOutputFilePath = config.general.outputFilePath!!
            ))
        }
    }

    private fun execute(body: () -> Unit) {
        try {
            body()
        } catch (e: ShowHelpException) {
            e.printAndExit(name, 80)
        } catch (e: ShowVersionException) {
            e.printAndExit(null, 80)
        } catch (e: SystemExitException) {
            val writer = OutputStreamWriter(System.err)
            e.printUserMessage(writer, name, 80)
            writer.writeln()
            writer.write("To see all the options, start: $name --help")
            writer.flush()
        } finally {
        }
    }

    private fun loadConfig(args: Array<String>): Config {
        val parser = ArgParser(args, helpFormatter = AppHelpFormatter)
        val config = Config(parser)
        config.validate()

        configureLogging(config.logging)

        return config
    }

    private fun configureLogging(logging: LoggingConfig) {
        val loggerContext = org.slf4j.LoggerFactory.getILoggerFactory() as LoggerContext
        val logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME)

        logger.detachAppender("console")

        val encoder = PatternLayoutEncoder()
        encoder.pattern = "%date %level %logger: %msg%n"
        encoder.context = loggerContext
        encoder.start()

        val appender = if (logging.logPath != null) {
            createFileAppender(encoder, loggerContext, logging)
        } else {
            createConsoleAppender(encoder, loggerContext, logging)
        }

        logger.addAppender(appender)

        logger.level = when (logging.verbosity) {
            0 -> Level.INFO
            1 -> Level.DEBUG
            else -> Level.ALL
        }

        if (logging.quiet) {
            logger.level = Level.OFF
        }

        loggerContext.addTurboFilter(object : TurboFilter() {
            override fun decide(marker: Marker?, logger: ch.qos.logback.classic.Logger?, level: Level?, format: String?, params: Array<out Any>?, t: Throwable?): FilterReply {
                return if (logger?.name?.startsWith("com.mongodb.migrator") == false) {
                    FilterReply.DENY
                } else FilterReply.NEUTRAL
            }
        })
    }

    private fun createConsoleAppender(encoder: Encoder<ILoggingEvent>, loggerContext: LoggerContext, logging: LoggingConfig): Appender<ILoggingEvent> {
        val appender = ConsoleAppender<ILoggingEvent>()
        appender.target = "System.err"
        appender.encoder = encoder
        appender.context = loggerContext
        appender.start()

        return appender
    }

    private fun createFileAppender(encoder: Encoder<ILoggingEvent>, loggerContext: LoggerContext, logging: LoggingConfig): Appender<ILoggingEvent> {
        val appender = FileAppender<ILoggingEvent>()
        appender.file = logging.logPath
        appender.encoder = encoder
        appender.context = loggerContext
        appender.start()

        return appender
    }
}

internal object AppHelpFormatter : HelpFormatter {
    override fun format(progName: String?, columns: Int, values: List<HelpFormatter.Value>): String {
        val builder = StringBuilder()

        builder.appendln("usage: $progName [OPTIONS]")

        values
            .filter { !it.isPositional }
            .map { value ->
                val idx = value.help.indexOf(':')
                val category = if (idx == -1) "General" else value.help.substring(0, idx)
                val help = if (idx == -1) value.help else value.help.substring(idx + 2)
                Triple(category, help, value)
            }
            .groupBy { it.first }
            .filter { it.key != "hidden" }
            .forEach { group ->
                builder.appendln()
                builder.appendln(group.key + " Options:")
                group.value.sortedBy { it.third.usages[0].trim('-') }.forEach { (_, second, third) ->
                    builder.appendln(third.usages.joinToString { it })
                    builder.appendln("\t\t$second")
                }
            }

        return builder.toString()
    }
}

fun Writer.writeln() {
    write(System.lineSeparator())
}

fun Writer.writeln(text: String) {
    write(text)
    write(System.lineSeparator())
}

