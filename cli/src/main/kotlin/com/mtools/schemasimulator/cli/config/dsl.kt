package com.mtools.schemasimulator.cli.config

import com.mtools.schemasimulator.executor.Simulation
import java.util.*
import java.util.concurrent.LinkedBlockingDeque

@DslMarker
annotation class HelperMarker

@HelperMarker
sealed class Helper<T> {
    abstract fun build(): T
}

fun config(init: ConfigHelper.() -> Unit): Config {
    val b = ConfigHelper()
    b.init()
    return b.build()
}

class ConfigHelper : Helper<Config>() {
    private lateinit var mongo: MongoConfig
    private lateinit var coordinator: CoordinatorConfig

    override fun build(): Config {
        return Config(mongo, coordinator)
    }

    fun mongodb(init: MongoHelper.() -> Unit = {}) {
        val b = MongoHelper()
        b.init()
        mongo = b.build()
    }

    fun coordinator(init: CoordinatorHelper.() -> Unit) {
        val b = CoordinatorHelper()
        b.init()
        coordinator = b.build()
    }
}

class CoordinatorHelper: Helper<CoordinatorConfig>() {
    private val tickers: MutableList<SlaveTickerConfig> = mutableListOf()
    private var tickResolutionMilliseconds: Long = 1L
    private var runForNumberOfTicks: Long = 1000L

    override fun build(): CoordinatorConfig {
        return CoordinatorConfig(LinkedBlockingDeque(tickers), tickResolutionMilliseconds, runForNumberOfTicks)
    }

    fun tickResolutionMilliseconds(value: Long) {
        tickResolutionMilliseconds = value
    }

    fun runForNumberOfTicks(value: Long) {
        runForNumberOfTicks = value
    }

    fun local(init: LocalHelper.() -> Unit) {
        val b = LocalHelper()
        b.init()
        tickers += b.build()
    }

    fun remote(init: RemoteHelper.() -> Unit) {
        val b = RemoteHelper()
        b.init()
        tickers += b.build()
    }
}

class RemoteHelper: Helper<SlaveTickerConfig>() {
    private lateinit var loadPatternConfig: LoadPatternConfig
    private lateinit var simulation: Simulation
    private var name: String = "remote_${Date().time}"

    override fun build(): SlaveTickerConfig {
        return RemoteConfig(name, loadPatternConfig, simulation)
    }

    fun name(value: String) {
        name = value
    }

    fun constant(init: ConstantHelper.() -> Unit) {
        val b = ConstantHelper()
        b.init()
        loadPatternConfig = b.build()
    }

    fun simulation(simulation: Simulation) {
        this.simulation = simulation
    }
}

class LocalHelper: Helper<SlaveTickerConfig>() {
    private lateinit var loadPatternConfig: LoadPatternConfig
    private lateinit var simulation: Simulation
    private var name: String = "local_${Date().time}"

    override fun build(): SlaveTickerConfig {
        return LocalConfig(name, loadPatternConfig, simulation)
    }

    fun name(value: String) {
        name = value
    }

    fun constant(init: ConstantHelper.() -> Unit) {
        val b = ConstantHelper()
        b.init()
        loadPatternConfig = b.build()
    }

    fun simulation(simulation: Simulation) {
        this.simulation = simulation
    }
}

class ConstantHelper: Helper<LoadPatternConfig>() {
    private var numberOfCExecutions: Long = 0L
    private var executeEveryMilliseconds: Long = 0L

    fun numberOfCExecutions(value: Long) {
        this.numberOfCExecutions = value
    }

    fun executeEveryMilliseconds(value: Long) {
        this.executeEveryMilliseconds = value
    }

    override fun build(): LoadPatternConfig {
        return ConstantConfig(numberOfCExecutions, executeEveryMilliseconds)
    }
}

class MongoHelper: Helper<MongoConfig>() {
    private lateinit var url: String
    private lateinit var dbName: String

    override fun build(): MongoConfig {
        return MongoConfig(url, dbName)
    }

    fun url(url: String) { this.url = url }

    fun db(dbName: String) { this.dbName = dbName}
}

interface LoadPatternConfig

data class ConstantConfig(val numberOfCExecutions: Long,
     val executeEveryMilliseconds: Long
) : LoadPatternConfig

interface SlaveTickerConfig

data class LocalConfig(val name: String, val loadPatternConfig: LoadPatternConfig, val simulation: Simulation) : SlaveTickerConfig

data class RemoteConfig(val name: String, val loadPatternConfig: LoadPatternConfig, val simulation: Simulation) : SlaveTickerConfig

data class CoordinatorConfig(val tickers: LinkedBlockingDeque<SlaveTickerConfig>, val tickResolutionMiliseconds: Long, val runForNumberOfTicks: Long)

data class Config(val mongo: MongoConfig, val coordinator: CoordinatorConfig)

data class MongoConfig(val url: String, val dbname: String)
