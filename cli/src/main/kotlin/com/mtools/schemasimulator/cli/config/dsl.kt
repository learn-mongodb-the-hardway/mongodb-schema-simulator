package com.mtools.schemasimulator.cli.config

import com.mtools.schemasimulator.executor.Simulation

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
    override fun build(): Config {
        return Config()
    }

    fun mongodb(init: MongoHelper.() -> Unit = {}) {
        val b = MongoHelper()
        b.init()
        b.build()
    }

    fun mongodb(any: Any) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun coordinator(init: CoordinatorHelper.() -> Unit) {
        val b = CoordinatorHelper()
        b.init()
        b.build()
    }
}

class CoordinatorHelper: Helper<CoordinatorConfig>() {
    override fun build(): CoordinatorConfig {
        return CoordinatorConfig()
    }

    fun local(init: LocalHelper.() -> Unit) {
        val b = LocalHelper()
        b.init()
        b.build()
    }
}

class LocalHelper: Helper<LocalConfig>() {
    override fun build(): LocalConfig {
        return LocalConfig()
    }

    fun constant(init: ConstantHelper.() -> Unit) {
        val b = ConstantHelper()
        b.init()
        b.build()
    }

    fun simulation(simulation: Simulation) {
    }
}

class ConstantHelper: Helper<ConstantConfig>() {
    override fun build(): ConstantConfig {
        return ConstantConfig()
    }
}

class MongoHelper: Helper<MongoConfig>() {
    override fun build(): MongoConfig {
        return MongoConfig()
    }

    fun url(url: String) {}
    fun db(name: String) {}
}

class ConstantConfig

class LocalConfig

class CoordinatorConfig

class Config

class MongoConfig
