package com.mtools.schemasimulator.schemas

interface ActionValues

interface Action {
    fun execute(values: ActionValues) : Map<String, Any>
}
