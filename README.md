# Ticket Reservation Problem

Distributed ticket reservation.

## No-Seat problem
Ticket state is only number of ticket.

Further features
- Persist number ticket state
- Payment state (none -> reserve -> paid)
- Query current state
- Timeout when no result
- Retry when ticket failed

## Seat problem
Ticket need to identify seat (one-seat = one ticket)