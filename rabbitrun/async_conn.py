"""RabbitMQ, pika asyncio connection. It follows this example:
https://github.com/pika/pika/blob/master/examples/asyncio_consumer_example.py
"""

# -*- coding: utf-8 -*-

import json
import functools
import logging
import time
import pika

from pika.adapters.asyncio_connection import AsyncioConnection

from . import config

# todo(): delete
# import config

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class ConsumeMessage(object):
    """Consumes a JSON message. A subclass of this method has to be passed to
       AsyncConsumer. This subclass will have to implement a `process_msg`
       method.
    """
    def __init__(self,
                 properties: pika.spec.BasicProperties = None,
                 body: bytes = None):

        self.payload = json.loads(body)
        self.properties = properties

    def __call__(self):

        self.process_msg(payload=self.payload, properties=self.properties)

    def process_msg(self, payload: dict = None,
                    properties: pika.spec.BasicProperties = None):

        raise NotImplementedError()


class AsyncConsumer(object):

    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self,
                 amqp_url: (str, pika.ConnectionParameters,) = None,
                 consume_class: ConsumeMessage = None):
        """Creatign a new consummer with a amqp url or an instance of 
        pika.ConnectionParameters
        :param str amqp_url: The AMQP url to connect with
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self.consume_class = consume_class

    def connect(self):
        """Connecting to RabbitMQ
        """
        LOGGER.info('Connecting to %s', self._url)
        return AsyncioConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """Called when the conneciton is open.
        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """Called when the connection can't be made.
        """
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """Called when the connection to is closed unexpectedly.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):

        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ
        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """Invoked when the channel is open.
        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):

        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel: pika.channel.Channel = None,
                          reason: Exception = None):
        """Invoked by the channel closes.

        :param pika.channel.Channel: The closed channel
        :param Exception: reason why the channel is closed
        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):

        LOGGER.info('Declaring exchange: %s', exchange_name)

        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, 
                              _unused_frame: pika.frame.Method = None,
                              userdata: str = None):
        """Invoked when RabbitMQ has finished the Exchange.Declare RPC
        command.
        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name: str = None):
        """Setup the queue on RabbitMQ. """
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame: pika.frame.Method = None,
                           userdata: str = None):
        """Method invoked when the Queue.Declare RPC call made in setup_queue 
        has completed. 
        """
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, queue_name,
                    self.ROUTING_KEY)
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame: pika.frame.Method = None,
                  userdata: str = None):
        """Invoked by pika when the Queue.Bind method has completed. """
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):

        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame: pika.frame.Method = None):
        """Invoked by when the Basic.QoS method has completed. """
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """Start the consummer. """

        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Callback invoked when RabbitMQ cancels the consumer. """

        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame: pika.frame.Method = None):
        """Called when RabbitMQ sends a Basic.Cancel. """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(
        self,
        _unused_channel: pika.channel.Channel = None,
        basic_deliver: pika.spec.Basic.Deliver = None,
        properties: pika.spec.BasicProperties = None,
        body: bytes = None):
        """Called when a message arrives from rabbitmq.

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.spec.Basic.Deliver: basic_deliver method
        :param pika.spec.BasicProperties: properties
        :param bytes body: The message body
        """
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

        self.consume_class(properties=properties, body=body)()

    def acknowledge_message(self, delivery_tag):

        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Send the Basic.Cancel to stop consumming."""
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame: pika.frame.Method = None, 
                    userdata: str = None):
        """When a cancel succeeds. """
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        """Close the channel."""
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the consummer.
        
        Start the IOLoop, and allow the AsyncioConnection to operate forever.
        """
        self._connection = self.connect()
        self._connection.ioloop.run_forever()

    def stop(self):
        """ Shutdown the connection to RaabbitMQ. """
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')


class ConnectAsyncConsumer(object):
    """A consumer that reconnects if the nested AsyncConsumer indicates that a
       reconnection is necessary.
    """

    def __init__(self,
                 amqp_url: str = None,
                 consume_class: ConsumeMessage = None):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._consumer = AsyncConsumer(
            amqp_url=self._amqp_url, consume_class=consume_class)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = AsyncConsumer(self._amqp_url)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > config.RECONNECT_DELAY:
            self._reconnect_delay = config.RECONNECT_DELAY
        return self._reconnect_delay



def run(user: str = None,
        password: str = None,
        host: str = None,
        vhost: str = None,
        consume_class: ConsumeMessage = None):
    """Running the asyncio connection. 
    The format of the url passed to ConnectAsyncConsumer is as follow:
    'amqp://user:pass@host:5672/%2F'
    """
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    url = f"amqp://{user}:{password}@{host}:{config.RABBIT_PORT}/{vhost}"
    consumer = ConnectAsyncConsumer(
        amqp_url=url, consume_class=consume_class)
    consumer.run()


if __name__ == '__main__':
    run(user='guest', password='guest', host='localhost')
