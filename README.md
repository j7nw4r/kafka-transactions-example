# kafka-transactions-example

## Purpose

This POC showcases the kafka transactional dataflow.

## Scenerios
We want to show that a request flow of:

- A transactional producer that commits successfully
- A transactional producer that aborts
- A transactional producer that also consumes under a transaction and commits.
- A transactional producer that also consumes under a transaction and aborts.
- A consumer that consumes transactionally


- Init Transaction, Produce, Consume read_committed, offset commit, commit transaction, Consume read_committed, offset commit
- Init Transaction, Produce, Consume read_uncommitted, commit transactions.
- Init Transaction, Produce, Consume read_uncommitted, abort transaction, Consume read_uncommitted