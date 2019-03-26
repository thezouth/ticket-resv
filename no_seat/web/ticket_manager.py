from asyncio import Future
import logging
import json
from typing import Dict, List

from confluent_kafka import Producer, Consumer, Message

from ..kafka import ProducerWrapper, ConsumerWrapper


class TicketRequester(ProducerWrapper):
    def __init__(self, requester_id, producer: Producer):
        super().__init__(producer)

        self.logger = logging.getLogger(__name__)
        self.requester_id = requester_id

    async def send_ticket_request(self, ticket_id: str):
        message = json.dumps({
            'ticketId': ticket_id,
            'requesterId': self.requester_id
        })

        result = await self._produce('no-seat-ticket-request', message.encode(), key=ticket_id)
        self.logger.debug('Produced %s', result)


class TicketResultWatcher(ConsumerWrapper):
    def __init__(self, requester_id, consumer: Consumer):
        super().__init__(consumer)
        self.subscribe(['no-seat-ticket-response'])
        self.requester_id = requester_id

        self.waiting_queue: Dict[str, Future] = dict()
        self.logger = logging.getLogger(__name__)

    def wait_for_response(self, ticket_id: str) -> Future:
        if ticket_id not in self.waiting_queue:
            self.waiting_queue[ticket_id] = Future()

        return self.waiting_queue[ticket_id]

    def _process(self, messages: List[Message]):
        for msg in messages:
            if not msg.error():
                result_message = json.loads(msg.value().decode())
                self.logger.debug('Got message: %s', result_message)

                if result_message['requesterId'] == self.requester_id:
                    self.logger.debug('I got result')
                    self.__trigger_ticket_response(result_message['ticketId'], result_message['success'])

    def __trigger_ticket_response(self, ticket_id: str, success: bool):
        if ticket_id not in self.waiting_queue:
            self.logger.warning('Received unknown ticket: %s', ticket_id)
            return

        future = self.waiting_queue[ticket_id]
        if not future.done():
            future.set_result(success)

        del self.waiting_queue[ticket_id]
