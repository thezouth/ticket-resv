import logging
from typing import Dict, List
import json
from time import time

from confluent_kafka import Consumer, Producer, Message, KafkaError
import click

from .kafka import ConsumerWrapper, ProducerWrapper

logger = logging.getLogger(__name__)


class TicketOfficer:
    def __init__(self, ticket_count):
        self.ticket_count = ticket_count

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
    def __init__(self, officer_id: int, remain_ticket: int,
                 consumer: Consumer, producer: Producer):
        super().__init__(consumer)
        self.subscribe(['no-seat-ticket-request'])

        self.officer = TicketOfficer(remain_ticket)

        self.state_reporter = StateReporter(officer_id, producer)
        self.result_reporter = TicketResultReporter(producer)

        self.logger = logging.getLogger(__name__)

    def _process(self, messages: List[Message]):
        messages = [msg for msg in messages if not msg.error()]

        before_state = self.officer.ticket_count

        for msg in messages:
            ticket_request = json.loads(msg.value().decode())
            ticket_id = ticket_request['ticketId']
            requester_id = ticket_request['requesterId']

            try:
                self.officer.ticket_please()
                self.result_reporter.report_success(ticket_id, requester_id)
            except Exception as ex:
                self.logger.error('Failed to process. %s', ex)
                self.result_reporter.report_failed(ticket_id, requester_id)

        if self.officer.ticket_count != before_state:
            self.state_reporter.report_state(self.officer.ticket_count)

    def _after_stop(self):
        self.result_reporter.flush()


class StateReporter:
    def __init__(self, officer_id: int, producer: Producer):
        self.producer = producer
        self.officer_id = str(officer_id)

    def report_state(self, remain_ticket: int):
        message = str(remain_ticket)
        self.producer.produce(
            'no-seat-remain-ticket',
            message,
            key=self.officer_id
        )
        self.producer.poll(0)


def get_ticket_remain(officer_id: int, consumer_config: Dict) -> int:
    officer_id_str = str(officer_id)

    consumer_config = consumer_config.copy()
    consumer_config.update({
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        'group.id': f'ticket-remain-restore-{officer_id_str}-{time()}'
    })

    consumer = Consumer(consumer_config)
    consumer.subscribe(['no-seat-remain-ticket'])

    remain_ticket = -1

    logger.debug('Start restore state loop.')
    for i in range(10):
        messages = consumer.consume(10000, timeout=1)
        found_eof = False

        for msg in messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    found_eof = True
                    break
                continue

            if msg.key().decode() == officer_id_str:
                remain_state = int(msg.value().decode())
                remain_ticket = min(remain_ticket, remain_state) if remain_ticket >= 0 else remain_state

        if found_eof:
            break
    else:
        logger.warning('Could not reach EoF of partition.')

    consumer.close()
    del consumer
    return remain_ticket


@click.command()
@click.option('--kafka-hosts', default='localhost:9092', help='Kafka servers (use "," to separate host)')
@click.option('-n', '--init-ticket', default='100', help='Initial Number of Ticket')
@click.option('--id', 'officer_id')
def start(kafka_hosts, init_ticket, officer_id):
    logging.basicConfig(level=logging.DEBUG)
    logger.info(f'Officer id={officer_id} is starting')
    consumer_config = {
        'bootstrap.servers': kafka_hosts,
        'group.id': 'ticket-office'
    }

    remain_ticket = get_ticket_remain(officer_id, consumer_config)
    if remain_ticket < 0:
        remain_ticket = init_ticket
        logger.info(f'There is not stored remaining ticket count. Start with ticket_count={init_ticket}')
    else:
        logger.info(f'Ticket was remain (in kafka state) = {remain_ticket}')

    producer_config = {
        'bootstrap.servers': kafka_hosts
    }

    processor = TicketProcessor(
        officer_id, remain_ticket,
        Consumer(consumer_config), Producer(producer_config)
    )

    try:
        processor.start_consumer_loop()
        processor.wait_until_stop()
    except KeyboardInterrupt:
        processor.stop_consumer_loop()
        processor.wait_until_stop()


if __name__ == '__main__':
    start()
