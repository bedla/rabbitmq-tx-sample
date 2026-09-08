package cz.publicstaticvoidmain.rabbitmq.rabbitmqtxsample

import org.testcontainers.junit.jupiter.Container
import org.testcontainers.rabbitmq.RabbitMQContainer
import org.testcontainers.utility.DockerImageName

class RabbitMQV3TxTest : AbstractRabbitMQTxTest() {
    override val rabbit: RabbitMQContainer
        get() = rabbitContainer


    companion object {
        @Container
        @JvmStatic
        val rabbitContainer = RabbitMQContainer(DockerImageName.parse("rabbitmq:3-management"))
    }
}