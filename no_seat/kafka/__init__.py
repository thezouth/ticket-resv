import asyncio
from contextlib import suppress
import logging
from threading import Thread
from typing import List

from confluent_kafka import Producer, Consumer, Message


logger = logging.getLogger(__name__)


def start_auto_poll(producer) -> asyncio.tasks:
    async def __auto_poll():
        logger.debug('Auto poll started.')
        with suppress(asyncio.CancelledError):
            while True:
                producer.poll(0)
                await asyncio.sleep(0.1)

    return asyncio.ensure_future(__auto_poll())


class ProducerWrapper:
    def __init__(self, producer: Producer):
        self.producer = producer

    async def _produce(self, topic: str, value: bytes, *args, **kwargs):
        future = asyncio.Future()
        kwargs['callback'] = ProducerWrapper.__callback_wrapper(future)

        self.producer.produce(topic, value, *args, **kwargs)

        return await future

    @staticmethod
    def __callback_wrapper(future: asyncio.Future):
        def __callback(err, result):
            if err:
                future.set_exception(err)
            else:
                future.set_result(result)

        return __callback


class ConsumerWrapper:
    def __init__(self, consumer: Consumer, batch_size: int=100):
        self.consumer = consumer

        self.thread: Thread = None
        self.running: bool = False

        self.batch_size = batch_size

    def subscribe(self, topics):
        self.consumer.subscribe(topics)

    def consume_once(self, num_messages=1) -> Message:
        return self.consumer.consume(num_messages, 0.1)

    def start_consumer_loop(self):
        if self.running or self.thread:
            logger.error('Consumer was started more than once.')
            raise RuntimeError('Consumer has started.')

        self.thread = Thread(target=self.__consumer_loop)
        self.running = True
        self.thread.start()
        logger.info('Consumer was started.')

    def stop_consumer_loop(self):
        logger.info('Consumer got stop signal.')
        self.running = False

    def wait_until_stop(self):
        self.thread.join()
        logger.info('Consumer was stopped.')

    def _process(self, messages: List[Message]):
        raise NotImplementedError('Expect child object implement this one.')

    def __consumer_loop(self):
        while self.running:
            messages = self.consumer.consume(self.batch_size, 0.1)
            self._process(messages)
