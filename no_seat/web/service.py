import logging
import uuid

import click
from confluent_kafka import Producer, Consumer
from quart import Quart, jsonify

from .ticket_manager import TicketRequester, TicketResultWatcher
from .. import kafka


app = Quart('no-seat-ticker')

ticket_office: TicketRequester = None
ticket_result_watcher: TicketResultWatcher = None
poll_task = None


@app.route('/ticket', methods=['POST'])
async def reserve_ticker():
    ticket_id = str(uuid.uuid4())
    future_result = ticket_result_watcher.wait_for_response(ticket_id)

    await ticket_office.send_ticket_request(ticket_id)

    success = await future_result
    if success:
        return jsonify({'ticketId': ticket_id})
    else:
        return jsonify({'error': 'Out of ticket.'}), 410


def __init_producer(kafka_host: str, requester_id: str):
    producer = Producer({'bootstrap.servers': kafka_host})
    return TicketRequester(requester_id, producer)


def __init_consumer(kafka_host: str, requester_id: str):
    consumer = Consumer({
        'bootstrap.servers': kafka_host,
        'group.id': 'ticket-requester-' + requester_id
    })
    return TicketResultWatcher(requester_id, consumer)


@app.before_serving
async def init():
    global poll_task
    poll_task = kafka.start_auto_poll(ticket_office.producer)
    ticket_result_watcher.start_consumer_loop()


@app.after_serving
async def stop():
    ticket_office.flush()

    if poll_task:
        poll_task.cancel()

    ticket_result_watcher.stop_consumer_loop()
    ticket_result_watcher.wait_until_stop()


@click.command()
@click.option('--id', 'requester_id', type=int)
@click.option('--kafka-hosts', default='localhost:9092',
              help='Kafka servers (use "," to separate host)')
@click.option('-p', '--http-port', default=5000, type=int)
def start_service(requester_id, kafka_hosts, http_port):
    logging.basicConfig(level=logging.DEBUG)

    global ticket_office
    global ticket_result_watcher
    ticket_office = __init_producer(kafka_hosts, str(requester_id))
    ticket_result_watcher = __init_consumer(kafka_hosts, str(requester_id))

    app.run('0.0.0.0', port=http_port)


if __name__ == '__main__':
    start_service()
