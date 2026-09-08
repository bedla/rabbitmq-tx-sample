package cz.publicstaticvoidmain.rabbitmq.rabbitmqtxsample

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RabbitmqTxSampleApplication

fun main(args: Array<String>) {
    runApplication<RabbitmqTxSampleApplication>(*args)
}
