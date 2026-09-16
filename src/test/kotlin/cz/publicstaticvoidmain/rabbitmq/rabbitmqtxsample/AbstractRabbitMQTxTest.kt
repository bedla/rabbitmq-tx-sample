package cz.publicstaticvoidmain.rabbitmq.rabbitmqtxsample

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.testcontainers.junit.jupiter.Testcontainers
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
@Testcontainers
abstract class AbstractRabbitMQTxTest {

    private lateinit var connection: Connection
    private lateinit var setupChannel: Channel

    private val inputQueue = "in.queue"
    private val outputQueue = "out.queue"

    abstract val rabbit: org.testcontainers.rabbitmq.RabbitMQContainer

    @BeforeEach
    fun setUp() {
        val factory = ConnectionFactory().apply {
            host = rabbit.host
            port = rabbit.getMappedPort(5672)
            username = rabbit.adminUsername
            password = rabbit.adminPassword
            requestedHeartbeat = 5
            connectionTimeout = 5.seconds.inWholeMilliseconds.toInt()
        }

        connection = factory.newConnection()
        setupChannel = connection.createChannel()

        setupChannel.queueDelete(inputQueue)
        setupChannel.queueDelete(outputQueue)

        setupChannel.queueDeclare(inputQueue, true, false, false, null)
        setupChannel.queueDeclare(outputQueue, true, false, false, null)
    }

    @AfterEach
    fun tearDown() {
        try {
            if (setupChannel.isOpen) setupChannel.close()
        } catch (_: Exception) {
        }
        try {
            if (connection.isOpen) connection.close()
        } catch (_: Exception) {
        }
    }

    @Test
    fun `producer sends message, worker consumes in tx and publishes processed message`() {
        publish(connection, inputQueue, "hello")

        // 2) worker (consumer+producer) in transaction
        val worker = TxWorker(connection, inputQueue, outputQueue)

        val done = CountDownLatch(1)
        worker.startOneShot(onProcessed = { done.countDown() })

        assertTrue(done.await(5, TimeUnit.SECONDS), "Worker did not process message in time")

        val outMsg = getSingleMessage(connection, outputQueue)
        assertEquals("processed:hello", outMsg)

        val inMsg = getSingleMessage(connection, inputQueue)
        assertNull(inMsg, "Message was not consumed")
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "1s",
            "5s",
            "15s",
        ]
    )
    fun `long running transaction`(durationStr: String) {
        val duration = kotlin.time.Duration.parse((durationStr))

        publish(connection, inputQueue, "hello")

        // 2) worker (consumer+producer) in transaction
        val worker = TxWorker(connection, inputQueue, outputQueue, onBeforeAck = { Thread.sleep(duration.toJavaDuration()) })

        val done = CountDownLatch(1)
        worker.startOneShot(onProcessed = { done.countDown() })

        assertTrue(done.await((duration + 5.seconds).inWholeMilliseconds, TimeUnit.MILLISECONDS), "Worker did not process message in time")

        val outMsg = getSingleMessage(connection, outputQueue)
        assertEquals("processed:hello", outMsg)

        val inMsg = getSingleMessage(connection, inputQueue)
        assertNull(inMsg, "Message was not consumed")
    }

    @Test
    fun `onBeforePublish - when processing fails transaction is rolled back output is empty and input remains`() {
        runFailTest {
            TxWorker(connection, inputQueue, outputQueue, onBeforePublish = { msg ->
                throw RuntimeException("onBeforePublish failed for: $msg")
            })
        }
    }

    @Test
    fun `onBeforeAck - when processing fails transaction is rolled back output is empty and input remains`() {
        runFailTest {
            TxWorker(connection, inputQueue, outputQueue, onBeforeAck = { msg ->
                throw RuntimeException("onBeforeAck failed for: $msg")
            })
        }
    }

    @Test
    fun `onBeforeCommit - when processing fails transaction is rolled back output is empty and input remains`() {
        runFailTest {
            TxWorker(connection, inputQueue, outputQueue, onBeforeCommit = { msg ->
                throw RuntimeException("onBeforeCommit failed for: $msg")
            })
        }
    }

    private fun runFailTest(createTxWorker: () -> TxWorker) {
        val message = "boom_${Uuid.generateV7()}"
        publish(connection, inputQueue, message)

        val worker = createTxWorker()

        val attempted = CountDownLatch(1)
        worker.startOneShot(onProcessed = { attempted.countDown() }, onError = { attempted.countDown() })

        assertTrue(attempted.await(10, TimeUnit.SECONDS), "Worker did not attempt processing in time")

        val outMsg = getSingleMessage(connection, outputQueue)
        assertNull(outMsg, "Output queue should be empty because tx was rolled back")

        val inMsg = getSingleMessage(connection, inputQueue)
        assertEquals(message, inMsg)
    }

    private fun publish(connection: Connection, queue: String, body: String) {
        connection.createChannel().use { ch ->
            ch.basicPublish("", queue, null, body.toByteArray(StandardCharsets.UTF_8))
        }
    }

    private fun getSingleMessage(connection: Connection, queue: String): String? {
        connection.createChannel().use { ch ->
            val resp = ch.basicGet(queue, true) ?: return null
            return String(resp.body, StandardCharsets.UTF_8)
        }
    }
}

private class TxWorker(
    private val connection: Connection,
    private val inputQueue: String,
    private val outputQueue: String,
    private val onBeforePublish: ((String) -> Unit)? = null,
    private val onBeforeAck: ((String) -> Unit)? = null,
    private val onBeforeCommit: ((String) -> Unit)? = null,
    private val processor: (String) -> String = { msg ->
        "processed:$msg"
    },
) {
    fun startOneShot(onProcessed: (() -> Unit)? = null, onError: ((Throwable) -> Unit)? = null) {
        val channel = connection.createChannel()

        // start transaction on channel (producer + consumer)
        channel.txSelect()
        channel.basicQos(1)

        val autoAck = false

        val consumerTag = channel.basicConsume(
            inputQueue,
            autoAck,
            { tag, delivery ->
                val deliveryTag = delivery.envelope.deliveryTag
                val input = String(delivery.body, StandardCharsets.UTF_8)

                try {
                    val result = processor(input)

                    onBeforePublish?.invoke(input)
                    channel.basicPublish(
                        "",
                        outputQueue,
                        AMQP.BasicProperties.Builder().deliveryMode(1).build(),
                        result.toByteArray(StandardCharsets.UTF_8)
                    )

                    onBeforeAck?.invoke(input)
                    channel.basicAck(deliveryTag, false)

                    onBeforeCommit?.invoke(input)
                    channel.txCommit()
                    onProcessed?.invoke()

                    // one-shot: close after first message
                    channel.basicCancel(tag)
                    channel.close()
                } catch (t: Throwable) {
                    try {
                        channel.txRollback()
                    } catch (_: Throwable) {
                    }
                    onError?.invoke(t)

                    // one-shot: close when error occured
                    channel.basicCancel(tag)
                    channel.close()
                }
            },
            { }
        )
        println("consumerTag=$consumerTag")
    }
}