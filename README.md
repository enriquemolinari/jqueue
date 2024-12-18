# JQueue - An extremely lightweight Relational Database Java Queue

![CI](https://github.com/enriquemolinari/jqueue/actions/workflows/tests.yml/badge.svg) [![codecov](https://codecov.io/gh/enriquemolinari/jqueue/branch/main/graph/badge.svg?token=GXRDRAK5GH)](https://codecov.io/gh/enriquemolinari/jqueue) [![Maintainability](https://api.codeclimate.com/v1/badges/c5c3e4a53ba6faf2d9cc/maintainability)](https://codeclimate.com/github/enriquemolinari/jqueue/maintainability) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/b53906357ca24c369a3d23cffbad231c)](https://www.codacy.com/gh/enriquemolinari/jqueue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=enriquemolinari/jqueue&utm_campaign=Badge_Grade)

## Why JQueue ?

In Microservices / Event Based architectures it is usually required (common pattern) for each service to make changes to their own database and additionally publish an event that might be consumed by other services. That must be done in a consistent way, wrapping both operations in a Tx. One elegant way to solve this is by using the Outbox Pattern. JQueue helps to implement this. By using JQueue, if your service’s database uses a relational database, you are able to wrap both operations in a transaction. This is possible because the JQueue is implemented using a relational database table to store queue tasks.

JQueue also provides a way to obtain the tasks from the queue and for instance to re-publishing them to RabbitMQ/Kafka or just doing any other talk with other external services like sending any type of notifications, integrating or replicating data to other databases (like NoSQL), etc.

JQueue was inspired by the beautiful and simple Yii2/php library called [Yii2 Queue](https://github.com/yiisoft/yii2-queue/).

## How to use it ?

Add the dependency to your project:

```xml
<dependency>
  <groupId>io.github.enriquemolinari</groupId>
  <artifactId>jqueue</artifactId>
  <version>0.0.2</version>
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

Your jobs must implement the `Job` interface. In the case that your Job instance throw an Exception, the task is pushed back into the queue and their processing delayed by five minutes. You can use any job scheduling library to check and execute JQueue entries frequently.

## Push Events Atomically (in a Tx)

It is the essence of this library to push events **atomically** within your business logic. Below, you will find some examples where first there is some code for the creation of a user entity (and that represent your business logic, the work you do in your application) and after that, **within the same Tx**, you will find the push of the `NewUserEvent`.

### Using Plain JDBC

```java
Connection conn = connection();
try {
 conn.setAutoCommit(false);

 //your business logic first
 final PreparedStatement st = conn.prepareStatement(
        "insert into user(id, user_name, pwd, email) values(108,  'user1','anyPassword','user1@dot.com')");
 st.executeUpdate();

 //then push an event
 JTxQueue.queue(conn)
     .push(new NewUserEvent(108, "user1", "user1@dot.com").toJson());

 conn.commit();
} catch (SQLException | JQueueException e) {
 try {
   conn.rollback();
   throw new RuntimeException(e);
 } catch (SQLException e1) {
    throw new RuntimeException(e1);
 }
} finally {
 try {
   conn.setAutoCommit(true);
   conn.close();
 } catch (SQLException e) {
   throw new RuntimeException(e);
 }
}
```

### Using Plain JPA/Hibernate

```java
EntityManagerFactory emf =
	Persistence.createEntityManagerFactory("...");
EntityManager em = emf.createEntityManager();
EntityTransaction tx = em.getTransaction();
try {
 tx.begin();
 //your business logic first
 User u = new User("username1", "pwd1", "user@dot.com");
 em.persist(u);
 //Then push an event
 Session session = em.unwrap(Session.class);
 session.doWork(new Work() {
  @Override
  public void execute(Connection connection) throws SQLException {
   JTxQueue.queue(connection)
        .push(new NewUserEvent(u.id(), u.userName(), u.email()).toJson());
  }
 });
 tx.commit();
} catch (Exception e) {
 tx.rollback();
 throw new RuntimeException(e);
} finally {
 if (em != null && em.isOpen())
  em.close();
 if (emf != null)
  emf.close();
}
```

### Using Spring

```java
@RestController
@RequestMapping("/api/users")
public class UserController {

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private DataSource dataSource;

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  @Transactional
  public User create(@RequestBody User user) throws SQLException {
    //your business logic first
    User u = userRepository.save(user);

    //then push an event
    JTxQueue.queue(dataSource)
        .push(new NewUserEvent(u.id(), u.getUserName(), u.email()).toJson());

    return u;
  }
}
```

## Requirements

JQueue currently supports PostgreSQL 9.5+ and MySQL 8.0+. To work properly, it uses the `select for update skip locked` which is a feature that some relational databases have incorporated few years ago. This feature eliminates any type of contention that might occur when queues are implemented using SQL.

JQueue requires the following table in your data store:

```sql
CREATE TABLE ar_cpfw_jqueue
(
  id int NOT NULL auto_increment, --MySQL
--  id serial,				      --PostgreSQL
--  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), --Derby
  channel varchar(100) NOT NULL,
  data text NOT NULL, --Derby does not have text datatype, use CLOB
  attempt int,
  delay int,
  pushed_at timestamp,
  CONSTRAINT id_pk PRIMARY KEY (id)
);

CREATE INDEX channel ON ar_cpfw_jqueue (channel);
```

The name of the table `ar_cpfw_jqueue` can be any other of your choice. Then use the correct factory method (`JQueueRunner.runner` and `JTxQueue.queue`) to pass the name of the table you have chosen and created.
