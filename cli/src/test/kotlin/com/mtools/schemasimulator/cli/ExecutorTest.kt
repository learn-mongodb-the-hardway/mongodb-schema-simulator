package com.mtools.schemasimulator.cli

import com.mtools.schemasimulator.cli.config.config

class ExecutorTest {

    fun correctlyParseBasicConfig() {

    }

    val simpleConfig = config {
        mongodb {
            url("mongoodb://locahost:27017")
            db("integration_tests")
        }

        // Master level coordinator
        coordinator {
            // Local running slave thread
            local {
                // Load Pattern
                constant {}

                // Simulation
                simulation(
                    SimpleSimulation(seedUserId = 1, numberOfDocuments = 10)
                )
            }
        }
    }
}
