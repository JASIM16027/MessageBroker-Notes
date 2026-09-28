# Real-World Projects (১০–১৬) — সমস্যা ও সমাধান কোডসহ

### সমস্যা ১০: Reprocess পুরনো ডেটা — bug fix-এর পর replay

**সমস্যা**: একটা consumer-এ bug ছিল, গত ২ দিনের event ভুলভাবে process হয়েছে। ঠিক করার পর ওই ডেটা আবার প্রসেস করতে হবে।

**সমাধান**: consumer group-এর offset পিছিয়ে দিন (reset) — Kafka-তে ডেটা এখনো log-এ আছে বলে সম্ভব।

```bash
# consumer বন্ধ করে group offset পিছিয়ে দাও, তারপর আবার চালাও
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group orders-worker --topic orders \
  --reset-offsets --to-datetime 2026-07-23T00:00:00.000 --execute
```

#### 🔍 আরও গভীরে
- এটাই Kafka বেছে নেওয়ার অন্যতম বড় কারণ — RabbitMQ-তে মেসেজ already মুছে গেছে, replay অসম্ভব।
- **নতুন group** দিয়ে reprocess করলে (`--to-earliest`) মূল pipeline অক্ষত রেখে যাচাই করা যায়।

### সমস্যা ১১: Real-time aggregation — গত ৫ মিনিটের count/sum

**সমস্যা**: প্রতি ৫ মিনিটে প্রতিটা পণ্যের বিক্রি গুনতে হবে, live dashboard-এ দেখাতে হবে।

**সমাধান**: **Kafka Streams** (বা ksqlDB) দিয়ে windowed aggregation — নিজে state store না বানিয়ে।

```java
// Kafka Streams (Java) — 5 মিনিটের tumbling window-এ প্রতি product-এর count
builder.stream("sales")
  .groupBy((k, v) -> v.productId())
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
  .count()
  .toStream()
  .to("sales.per.5min");     // ফল আরেকটা topic-এ → dashboard পড়ে
```

#### 🔍 আরও গভীরে
- Kafka Streams exactly-once দেয় (transaction), state RocksDB-তে রাখে আর changelog topic-এ backup করে — consumer crash করলে state rebuild হয়।
- ছোট কাজে ksqlDB (SQL syntax) আরও সহজ।

### সমস্যা ১২: Multi-Datacenter — cross-region replication

**সমস্যা**: Dhaka আর Singapore দুই datacenter; একটা down হলেও stream চালু রাখতে হবে, বা analytics এক জায়গায় আনতে হবে।

**সমাধান**: **MirrorMaker 2** (Kafka Connect-based) দিয়ে এক cluster-এর topic আরেক cluster-এ replicate করা।

```properties
# MirrorMaker 2 — dhaka cluster → singapore cluster
clusters = dhaka, singapore
dhaka.bootstrap.servers = dhaka-broker:9092
singapore.bootstrap.servers = sg-broker:9092
dhaka->singapore.enabled = true
dhaka->singapore.topics = orders, payments        # এই topic গুলো mirror হবে
# offset-ও translate হয়, তাই failover-এ consumer সঠিক জায়গা থেকে শুরু করে
```

#### 🔍 আরও গভীরে
- MM2 topic prefix দেয় (`dhaka.orders`), তাই কোনটা কোথা থেকে এসেছে বোঝা যায় ও loop এড়ানো যায়।
- offset translation থাকায় DR failover-এ consumer প্রায় সঠিক জায়গা থেকে resume করতে পারে।

### সমস্যা ১৩: Schema বদলালে consumer ভেঙে যাচ্ছে

**সমস্যা**: Producer event-এ নতুন field যোগ করলো, পুরনো consumer parse করতে গিয়ে crash করছে।

**সমাধান**: **Schema Registry** (Avro/Protobuf) দিয়ে compatibility enforce করা — backward-compatible পরিবর্তনই কেবল allow।

```js
// producer — schema registry দিয়ে serialize; incompatible schema হলে registry reject করবে
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const registry = new SchemaRegistry({ host: 'http://schema-registry:8081' });
const id = await registry.getLatestSchemaId('orders-value');
const encoded = await registry.encode(id, order);          // Avro binary
await producer.send({ topic: 'orders', messages: [{ key: order.id, value: encoded }] });
```

#### 🔍 আরও গভীরে
- **Backward compatibility**: নতুন consumer পুরনো ডেটা পড়তে পারে। **Forward**: পুরনো consumer নতুন ডেটা পড়তে পারে (নতুন field ignore করে)। default `BACKWARD`।
- Avro/Protobuf JSON-এর চেয়ে ছোট ও দ্রুত, তাই high-throughput-এ ভালো।

### সমস্যা ১৪: Consumer পিছিয়ে যাচ্ছে (Lag বাড়ছে)

**সমস্যা**: Traffic spike-এ consumer produce-এর গতি ধরতে পারছে না, lag বাড়ছে, ডেটা retention-এর কাছাকাছি পৌঁছে যাচ্ছে।

**সমাধান**: partition ও consumer বাড়ান (parallelism), per-message কাজ হালকা করুন, batching ব্যবহার করুন।

```js
// batch-এ process → কম commit overhead, বেশি throughput
await consumer.run({
  eachBatchAutoResolve: false,
  eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat }) => {
    await bulkInsert(batch.messages.map(m => JSON.parse(m.value.toString())));  // একসাথে
    for (const m of batch.messages) resolveOffset(m.offset);
    await commitOffsetsIfNecessary();
    await heartbeat();                       // দীর্ঘ batch-এ session timeout এড়াতে
  },
});
```

#### 🔍 আরও গভীরে
- **Lag alert**: `kafka-consumer-groups --describe --group X` বা Burrow/Prometheus দিয়ে monitor করুন।
- consumer বাড়ানোর সীমা = partition সংখ্যা। তাই আগে থেকে যথেষ্ট partition রাখা জরুরি।

### সমস্যা ১৫: Flash sale — inventory oversell ঠেকানো

**সমস্যা**: একই পণ্যের জন্য হঠাৎ হাজার হাজার order; oversell হয়ে যাচ্ছে।

**সমাধান**: `productId` কে key বানিয়ে একই পণ্যের সব order একই partition-এ, একজন consumer serially process করুক — race condition দূর।

```js
await producer.send({ topic: 'reserve', messages: orders.map(o =>
  ({ key: o.productId, value: JSON.stringify(o) })) });    // productId → একই partition

// consumer: একই partition = serial → একই productId-এ কোনো concurrent decrement নেই
await consumer.run({ eachMessage: async ({ message }) => {
  const o = JSON.parse(message.value.toString());
  if (await decrementIfAvailable(o.productId)) await confirm(o);   // atomic check-and-decrement
  else await reject(o, 'sold_out');
}});
```

#### 🔍 আরও গভীরে
- একই key → একই partition → serial processing — এটাই ordering সমাধানের (সমস্যা ৩) আরেক ব্যবহার, এখানে race condition ঠেকাতে।
- সত্যিকারের oversell protection-এ DB/Redis-এ atomic decrement লাগে; Kafka শুধু একই পণ্যের request গুলো এক লাইনে আনে।

### সমস্যা ১৬: Message হারানো ঠেকানো — end-to-end durability

**সমস্যা**: Broker crash-এ কিছু payment event হারিয়ে গেছে।

**সমাধান**: producer, topic, consumer — তিন দিকেই durability config ঠিক করুন।

```js
// producer: acks=all + idempotence + retries → confirm ছাড়া হারানো নয়
const producer = kafka.producer({ idempotent: true });     // acks=all, retries=MAX, in-flight নিরাপদ

// topic: replication.factor=3, min.insync.replicas=2 (broker/topic config)
//   → অন্তত ২ replica-তে না লেখা পর্যন্ত write সফল ধরা হবে না

// consumer: process-এর পরে manual commit → at-least-once
await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
  await persist(message);                                  // আগে কাজ শেষ
  await consumer.commitOffsets([{ topic, partition,
    offset: (Number(message.offset) + 1).toString() }]);   // তারপর commit
}});
```

#### 🔍 আরও গভীরে
- তিনটা layer একসাথে না হলে ফাঁক থাকে: `acks=1` হলে leader fail-এ হারায়; auto-commit হলে process-এর আগে commit হয়ে হারাতে পারে; RF=1 হলে broker fail-এ হারায়।
- এটাই RabbitMQ-র "durable queue + persistent message + publisher confirm"-এর Kafka সমতুল্য।

---

---

[⬅ 11-Real-World-Projects-Part1.md](./11-Real-World-Projects-Part1.md) | [13-Interview-Questions-All-Levels.md ➡](./13-Interview-Questions-All-Levels.md) | [🏠 Repo Home](../README.md)
