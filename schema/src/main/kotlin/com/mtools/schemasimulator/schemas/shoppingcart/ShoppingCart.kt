package com.mtools.schemasimulator.schemas.shoppingcart

import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.ScenarioProgress

abstract class ShoppingCart: Scenario {

    fun setup() {}

    fun teardown() {}
}

abstract class ReservationShoppingCart: ShoppingCart() {
    fun execute(configuration:  HashMap<String, Any>): ScenarioProgress {
        return ScenarioProgress()
    }
}

class SuccessFullShoppingCart: ReservationShoppingCart() {
}

class FailedShoppingCart: ReservationShoppingCart() {
}
