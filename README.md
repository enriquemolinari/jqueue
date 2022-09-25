# JQueue - An extremely lightweight Relational Database Java Queue

![CI](https://github.com/enriquemolinari/jqueue/actions/workflows/tests.yml/badge.svg) [![codecov](https://codecov.io/gh/enriquemolinari/jqueue/branch/main/graph/badge.svg?token=GXRDRAK5GH)](https://codecov.io/gh/enriquemolinari/jqueue) [![Maintainability](https://api.codeclimate.com/v1/badges/c5c3e4a53ba6faf2d9cc/maintainability)](https://codeclimate.com/github/enriquemolinari/jqueue/maintainability) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/b53906357ca24c369a3d23cffbad231c)](https://www.codacy.com/gh/enriquemolinari/jqueue/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=enriquemolinari/jqueue&amp;utm_campaign=Badge_Grade)

## Why JQueue ?

In a Microservices / Event Based architecture it is a common pattern to make changes to the vertical data store plus publishing an event to be consumed by other verticals. That must be done in a consistent way, wrapping both operations in a Tx. By using JQueue, if your verical’s data store uses a relational database, you are able to wrap both operations in a transaction. Then, another process might pull the events from the JQueue and publish them to RabbitMQ, Kafka or other systems like them. This is known as Outbox Pattern. 

## How to use it ?

To push something on the default channel of the queue you can do this:

´´´java
var jqueue = JTxQueue.queue(/*a JDBC Data Source or a JDBC Connection */);
jqueue.push({
   "type": "job_type1",
   "event":{
      "id": "an id",
      "value": ""
   }
});
´´´

To push something on an specific channel of the queue you can do this:

´´´java
var jqueue = JTxQueue.queue(/*a JDBC Data Source or a JDBC Connection */);
jqueue.channel("achannel").push({
   "type": "job_type1",
   "event":{
      "id": "an id",
      "value": ""
   }
});
´´´

The following snippet executes all the entries in the queue in a loop until is empty:

´´´java
var runner = JQueueRunner.runner(/* a JDBC DataSource */);

runner.executeAll(new Job() {
  @Override
  public void run(String data) {
	//do something with data
  }
});
´´´
Your jobs must implement the ´Job´ interface.

## Requirements

JQueue currently supports PostgreSQL 9.5+ and MySQL 8.0+. To work properly, it uses the `select for update skip locked` which is a feature that some relational databases have incorporated few years ago. This feature eliminates any type of contention that might occure when Queue are implemented using SQL.

´´´sql
CREATE TABLE ar_cpfw_jqueue
( 
  id char(36) NOT NULL,
  channel varchar(100) NOT NULL,
  data text NOT NULL,
  attempt int,
  delay int,
  pushed_at timestamp,
  CONSTRAINT id_pk PRIMARY KEY (id)
);

CREATE INDEX channel ON ar_cpfw_jqueue (channel); 
´´´
The name of the table ´ar_cpfw_jqueue´ can be any other of your choice. Then use the correct factory method (´´´JQueueRunner.runner´´´ and ´´´JTxQueue.queue´´´) to pass the name of the table you have chosen and created.  
