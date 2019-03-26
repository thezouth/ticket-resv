import logging
from time import time
from typing import List
import json

from confluent_kafka import Consumer, Producer, Message

from .kafka import ConsumerWrapper, ProducerWrapper

logger = logging.getLogger(__name__)


class TicketOfficer:
    def __init__(self):
        self.ticket_count = 40

    def ticket_please(self, cnt=1):
        logger.debug('Tickets remain %d, Ticket request %d', self.ticket_count, cnt)
        if self.ticket_count < cnt:
            raise Exception('No ticket.')

        self.ticket_count -= cnt
        return self.ticket_count


class TicketResultReporter(ProducerWrapper):
    def __init__(self, producer: Producer, result_partitions=12):
        super().__init__(producer)
        self.result_partitions = result_partitions

    def report_success(self, ticket_id: str, requester_id: int):
        self._report(ticket_id, requester_id, True)

    def report_failed(self, ticket_id: str, requester_id: int):
        self._report(ticket_id, requester_id, False)

    def _report(self, ticket_id: str, requester_id: int, success: bool):
        message = json.dumps({
            'ticketId': ticket_id,
            'requesterId': requester_id,
            'success': success
        })

        partition = int(requester_id) % self.result_partitions
        self.producer.produce('no-seat-ticket-response', message.encode(), partition=partition)


class TicketProcessor(ConsumerWrapper):
    def __init__(self, consumer: Consumer, producer: Producer):
        super().__init__(consumer)
        self.subscribe(['no-seat-ticket-request'])

        self.officer = TicketOfficer()
        self.reporter = TicketResultReporter(producer)

        self.logger = logging.getLogger(__name__)

    def _process(self, messages: List[Message]):
        messages = [msg for msg in messages if not msg.error()]

        for msg in messages:
            ticket_request = json.loads(msg.value().decode())
            ticket_id = ticket_request['ticketId']
            requester_id = ticket_request['requesterId']

            try:
                self.officer.ticket_please()
                self.reporter.report_success(ticket_id, requester_id)
            except Exception as ex:
                self.logger.error('Failed to process. %s', ex)
                self.reporter.report_failed(ticket_id, requester_id)

        # remain_ticket = self.officer.ticket_count
        #
        # if not remain_ticket:
        #     self.logger.info('Tickets was run out.')
        #     self.stop_consumer_loop()
        #
        # if remain_ticket <= self.batch_size:
        #     self.batch_size = remain_ticket


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'ticket-office',
        'auto.offset.reset': 'earliest'
    }

    producer_config = {
        'bootstrap.servers': 'localhost:9092'
    }

    processor = TicketProcessor(Consumer(consumer_config), Producer(producer_config))
    try:
        processor.start_consumer_loop()
        processor.wait_until_stop()
    except KeyboardInterrupt:
        processor.stop_consumer_loop()
        processor.wait_until_stop()
