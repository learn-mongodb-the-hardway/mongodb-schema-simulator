package com.mtools.schemasimulator.cli

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default
import java.io.File

class Config(parser: ArgParser) {
    val logging = LoggingConfig(parser)
    val general = GeneralConfig(parser)

    fun validate() {
    }
}

class LoggingConfig(parser: ArgParser) {
    val quiet by parser.flagging("--quiet", help = "Logging: turn off all logging").default(false)
    val verbosity by parser.counting("-v", "--verbosity", help = "Logging: verbosity of logging (repeatable)").default(0)
    val logPath by parser.storing("--logpath", help = "Logging: log file to send write to instead of stdout - has to be a file, not directory").default(null)
}

class GeneralConfig(parser: ArgParser) {
    val version by parser.option<Unit>("--version", help = "General: display the version") {
        throw ShowVersionException("version      : ${App.version}${System.lineSeparator()}git revision : ${App.gitRev}")
    }.default(false)

    val config by parser.storing("-c", "--config", help = "General: the directory and file for the input configuration")
        .default(null)
        .addValidator {
            // Value must be set
            if (value == null) {
                throw SystemExitException("option '--config must be specified'", 2)
            }

            // File must exist
            if (!File(value).exists()) {
                throw SystemExitException("option 'file [$value] specified for --config does not exist'", 2)
            }
        }

    val worker by parser.flagging("--worker", help = "Instance is a worker instance").default(false)

    val master by parser.flagging("--master", help = "Instance is a master instance").default(false)

    val masterURI = parser.storing("--master-uri", help = "Connection: master <host:port> connection, example [--master-uri localhost:15440]")
        .default(null)
        .addValidator {
            // Value must be set
            if (value == null && worker) {
                throw SystemExitException("option '--master-uri must be specified if worker is set'", 2)
            }

            // Validate the connection uri passed in
            if (worker) {
                val parts = value!!.split(":")
                if (parts.size < 2) {
                    throw SystemExitException("option 'master-uri must be of the format <host:port>'", 2)
                }

                // Check if we can parse the port
                try {
                    parts[1].toInt()
                } catch(ex: Exception) {
                    throw SystemExitException("option 'master-uri port provided is not a number'", 2)
                }
            }
        }

    val host by parser.storing("-h", "--host", help = "General: The host we are binding too")
        .default(null)
        .addValidator {
            if (value == null && (master || worker)) {
                throw SystemExitException("option --port must be specified", 2)
            }
        }

    val port by parser.storing("-p", "--port", help = "General: The host we are binding too")
        .default(null)
        .addValidator {
            if (value == null && (master || worker)) {
                throw SystemExitException("option --port must be specified", 2)
            }

            // Check if we can parse the port
            if ((master || worker)) {
                try {
                    value!!.toInt()
                } catch (ex: Exception) {
                    throw SystemExitException("option '--port provided is not a number'", 2)
                }
            }
        }
}

class ShowVersionException(version: String) : SystemExitException(version, 0)
