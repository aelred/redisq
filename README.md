---
[![GitHub release](https://img.shields.io/github/release/graknlabs/redisq.svg)](https://github.com/graknlabs/redisq/releases/latest)
[![Build Status](https://travis-ci.org/graknlabs/redisq.svg?branch=internal)](https://travis-ci.org/graknlabs/redisq)
[![Slack Status](http://grakn-slackin.herokuapp.com/badge.svg)](https://grakn.ai/slack)

# Redisq

Redisq is a lightweight library that implements a simple publish/subscriber interface for reliable queues on Redis.

A Redisq object represents a queue. A _push_ adds a document to the queue and a _subscribe_ defines how documents are processed.

A Redis _brpoplpush_ moves the id of the document from a processing queue into an inflight queue and in case of unacknowledged failure
they are moved back into the processing queue after a timeout.

Documents are objects with an Id and are serialised/deserialised using [Jackson](https://github.com/FasterXML/jackson).


## Quickstart

Add it as a dependency in Maven as:

```xml
<dependencies>
    <dependency>
        <groupId>ai.grakn</groupId>
        <artifactId>redisq</artifactId>
        <version>0.0.5</version>
    </dependency> 
</dependencies>
```


First we need to create a serializable class with an id.

```java
public class DummyObject implements Document {
    @JsonProperty
    private String id;

    // Needed by Jackson
    public DummyObject() {}
    

    public DummyObject(String id) {
        this.id = id;
    }

    @Override
    @JsonIgnore
    public String getIdAsString() {
        return id;
    }
}
```

At the minimum the queue requires a Jedis pool that points to the Redis instance being used,
a name, the class of the document, and a consumer.

```java
Pool<Jedis> jedisPool = new JedisPool();

Queue<DummyObject> redisq = new RedisqBuilder<DummyObject>()
                .setJedisPool(jedisPool)
                .setName("my_queue")
                .setConsumer((d) -> System.out.println("I'm consuming " + d.getIdAsString()))
                .setDocumentClass(DummyObject.class)
                .createRedisq();
redisq.startConsumer();
```

We can implement a producer that pushes just one document and waits for 1 second that the document is
processed as follows.

```java
redisq.push(new DummyObject("documentid"));
```

Remember to close the queue:

```java
redisq.close();
```

## Advanced Usage

A producer can be instantiated by creating a queue without a consumer

```java
Redisq<DummyObject> redisq =  new RedisqBuilder<DummyObject>()
        .setName("my_queue")
        .setJedisPool(jedisPool)
        .setDocumentClass(DummyObject.class)
        .createRedisq();
// Don't start consumer
redisq.pushAndWait(new DummyObject("id123"), 5, TimeUnit.SECONDS);
```

The wait blocks the producer until the object is processed. Alternatively the push and the wait
can be performed separately
```java
Future<Void> f = redisq.getFutureForDocumentStateWait(DONE, "id123", 5, TimeUnit.SECONDS);
redisq.push(new DummyObject("id123"));
...
f.get();

```

Consumers need to call _startConsumer()_ explicitly after creation
 
 ```java
Queue<DummyObject> redisq = new RedisqBuilder<DummyObject>()
                .setName("my_queue")
                .setJedisPool(jedisPool)
                .setConsumer((d) -> System.out.println("I'm consuming " + d.getIdAsString()))
                .setDocumentClass(DummyObject.class)
                .createRedisq();
redisq.startConsumer();
```
