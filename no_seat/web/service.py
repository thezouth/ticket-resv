import logging
import uuid

from quart import Quart, jsonify
from confluent_kafka import Producer, Consumer

from .ticket_manager import TicketRequester, TicketResultWatcher
from .. import kafka

logging.basicConfig(level=logging.DEBUG)

app = Quart('no-seat-ticker')

producer = Producer({'bootstrap.servers': 'localhost:9092'})
ticket_office = TicketRequester('1', producer)
poll_task = None

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ticket-requester-1'
})
ticket_result_watcher = TicketResultWatcher('1', consumer)


@app.before_serving
async def start_kafka_auto_poll():
    kafka.start_auto_poll(producer)
    ticket_result_watcher.start_consumer_loop()


@app.after_serving
async def gracefully_stop_kafka():
    producer.flush()

    if poll_task:
        poll_task.cancel()

    ticket_result_watcher.stop_consumer_loop()
    ticket_result_watcher.wait_until_stop()


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


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)
