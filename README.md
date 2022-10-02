# JQueue - An extremely lightweight Relational Database Java Queue

![CI](https://github.com/enriquemolinari/jqueue/actions/workflows/tests.yml/badge.svg) [![codecov](https://codecov.io/gh/enriquemolinari/jqueue/branch/main/graph/badge.svg?token=GXRDRAK5GH)](https://codecov.io/gh/enriquemolinari/jqueue) [![Maintainability](https://api.codeclimate.com/v1/badges/c5c3e4a53ba6faf2d9cc/maintainability)](https://codeclimate.com/github/enriquemolinari/jqueue/maintainability) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/b53906357ca24c369a3d23cffbad231c)](https://www.codacy.com/gh/enriquemolinari/jqueue/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=enriquemolinari/jqueue&amp;utm_campaign=Badge_Grade)

## Why JQueue ?

In Microservices / Event Based architectures it is usually required (common pattern) for each service to make changes to their own database and additionally publish an event that might be consumed by other services. That must be done in a consistent way, wrapping both operations in a Tx. One elegant way to solve this is by using the Outbox Pattern. JQueue helps to implement this. By using JQueue, if your service’s database uses a relational database, you are able to wrap both operations in a transaction. This is possible because the JQueue is implemented using a relational database table to store queue tasks. 

JQueue also provides a way to obtain the tasks from the queue and for instance to re-publishing them to RabbitMQ/Kafka or just doing any other talk with other external services like sending any type of notifications, integrating or replicating data to other databases (like NoSQL), etc.

JQueue was inspired by the beautiful and simple Yii2/php library called [Yii2 Queue](https://github.com/yiisoft/yii2-queue/).


## How to use it ?

```xml
<dependency>
  <groupId>io.github.enriquemolinari</groupId>
  <artifactId>jqueue</artifactId>
  <version>0.0.1</version>
</dependency>
```

To push something on the default channel of the queue you can do this:

```java
JTxQueue.queue(/*a JDBC Data Source or a JDBC Connection */)
 .push(
   "{\"type\": \"job_type1\", \"event\":{\"id\": \"an id\", \"value\": \"\" }}");
```

To push something on an specific channel of the queue you can do this:

```java
JTxQueue.queue(/*a JDBC Data Source or a JDBC Connection */)
 .channel("achannel").push(
   "{\"type\": \"job_type1\", \"event\":{\"id\": \"an id\", \"value\": \"\" }}");
```

Make sure that the `dataSource` or `connection` you pass as argument to the `queue` factory method above is the one you use to open the transaction which then later will be committed or rolledback.

The following snippet executes all the entries in the queue in a loop until is empty:

```java
JQueueRunner.runner(/* a JDBC DataSource */)
 .executeAll(new Job() {
   @Override
   public void run(String data) {
	 //do something with data
   }
 });
```

Your jobs must implement the `Job` interface. You can use any job scheduling library to check and execute JQueue entries.

## Requirements

JQueue currently supports PostgreSQL 9.5+ and MySQL 8.0+. To work properly, it uses the `select for update skip locked` which is a feature that some relational databases have incorporated few years ago. This feature eliminates any type of contention that might occure when queues are implemented using SQL.

JQueue requires the following table in your data store:

```sql
CREATE TABLE ar_cpfw_jqueue
( 
  id int NOT NULL auto_increment, --MySQL
--  id serial,						  --PostgreSQL	
  channel varchar(100) NOT NULL,
  data text NOT NULL,
  attempt int,
  delay int,
  pushed_at timestamp,
  CONSTRAINT id_pk PRIMARY KEY (id)
);

CREATE INDEX channel ON ar_cpfw_jqueue (channel); 
```

The name of the table `ar_cpfw_jqueue` can be any other of your choice. Then use the correct factory method (`JQueueRunner.runner` and `JTxQueue.queue`) to pass the name of the table you have chosen and created.  
