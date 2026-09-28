# Real-World Projects (১–৯) — সমস্যা ও সমাধান কোডসহ


এই সেকশনে কয়েকটা বাস্তব প্রজেক্ট সমস্যা, আর Kafka দিয়ে কীভাবে সেগুলো সমাধান করা হয় — practical কোড (Node.js `kafkajs` / Python `confluent-kafka`) সহ দেখানো হলো।

### সমস্যা ১: Order event অনেক টিম লাগবে — point-to-point integration spaghetti

**সমস্যা**: Order placed হলে Inventory, Shipping, Email, Analytics, Fraud — ৫টা টিমের ডেটা লাগে। Order Service প্রতিটাকে আলাদা করে call করছে, নতুন consumer যোগ করলেই Order Service-এর কোড বদলাতে হচ্ছে।

**সমাধান**: Order Service শুধু একবার `orders` topic-এ event লেখে; প্রতিটা টিম নিজের consumer group দিয়ে independently পড়ে।

```js
// producer — Order Service, একবারই লেখে
const producer = kafka.producer({ idempotent: true });   // acks=all, dedup
await producer.connect();
await producer.send({
  topic: 'orders',
  messages: [{ key: order.id, value: JSON.stringify(order) }],  // key=orderId → ordering
  acks: -1,                                               // acks=all
});
```

```js
// consumer — প্রতিটা টিম আলাদা group, একই ডেটা independently পড়ে
const inventory = kafka.consumer({ groupId: 'inventory-svc' });
await inventory.subscribe({ topic: 'orders', fromBeginning: false });
await inventory.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    await reserveStock(JSON.parse(message.value.toString()));
    await inventory.commitOffsets([{ topic, partition,
      offset: (Number(message.offset) + 1).toString() }]);   // process-এর পরে commit
  },
});
// নতুন টিম যোগ করা = নতুন groupId; Order Service-এ কোনো পরিবর্তন লাগে না
```

#### 🔍 আরও গভীরে
- **Decoupling**: producer জানেও না কে কে পড়ছে — নতুন consumer শূন্য খরচে যোগ হয়। RabbitMQ-তে এটা করতে fanout exchange + প্রতি consumer-এ আলাদা queue (duplicate storage) লাগতো; Kafka-তে একই log সবাই শেয়ার করে।
- **key=orderId**: একই order-এর সব event একই partition-এ, একই order-এ (সেকশন ৫)।
- **manual commit**: process শেষে commit → at-least-once। consumer crash করলেও শেষ committed offset থেকে আবার শুরু, মেসেজ হারায় না।

### সমস্যা ২: একই ডেটা duplicate process হচ্ছে (Idempotency)

**সমস্যা**: At-least-once delivery-তে rebalance/retry-র কারণে একই order event দুবার আসতে পারে — ফলে দুবার stock কমছে বা দুবার email যাচ্ছে।

**সমাধান**: Consumer-কে **idempotent** বানান — একটা dedup key (event id) রেখে already-processed হলে skip করুন।

```js
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    const evt = JSON.parse(message.value.toString());
    // atomic: এই id আগে দেখা থাকলে insert fail → skip (DB unique constraint)
    const isNew = await db.tryInsertProcessed(evt.eventId);  // INSERT ... ON CONFLICT DO NOTHING
    if (isNew) await applyEffect(evt);                       // মূল কাজ একবারই হবে
    await consumer.commitOffsets([{ topic, partition,
      offset: (Number(message.offset) + 1).toString() }]);
  },
});
```

#### 🔍 আরও গভীরে
- Kafka exactly-once (transaction) দিয়েও করা যায়, কিন্তু downstream (DB, external API)-এ effect থাকলে **application-level idempotency**-ই সবচেয়ে নির্ভরযোগ্য ও সহজ।
- dedup key store: DB unique constraint, বা Redis `SETNX` with TTL।

### সমস্যা ৩: Payment order উল্টে যাচ্ছে — ordering ভাঙছে

**সমস্যা**: `payment_authorized` এর আগে `payment_captured` process হয়ে যাচ্ছে, কারণ event ভিন্ন partition-এ ছড়িয়ে গেছে।

**সমাধান**: একই `paymentId` কে partition key বানান — সব একই partition-এ, একই order-এ।

```js
await producer.send({
  topic: 'payments',
  messages: [
    { key: paymentId, value: JSON.stringify({ type: 'authorized', paymentId }) },
    { key: paymentId, value: JSON.stringify({ type: 'captured',  paymentId }) },
  ],
});
// idempotence on → retry হলেও order নষ্ট হয় না (max.in.flight নিরাপদ)
```

#### 🔍 আরও গভীরে
- Ordering guarantee **শুধু partition-এর ভেতরে** (সেকশন ৫)। key দিয়ে same-entity same-partition — এটাই কৌশল।
- `enable.idempotence=true` না থাকলে retry ordering ভাঙতে পারতো।

### সমস্যা ৪: বিশাল Clickstream — millions/sec ingest ও multi-team read

**সমস্যা**: প্রতি সেকেন্ডে লক্ষ লক্ষ click event; Analytics, Recommendation, Fraud — সবার লাগে; নতুন ML model-এর জন্য গত ৩ মাসের ডেটা replay দরকার।

**সমাধান**: high-partition topic + compression + আলাদা consumer group; replay-র জন্য offset reset।

```js
// producer — high throughput: batching + compression
const producer = kafka.producer();
await producer.send({
  topic: 'clicks',                       // যেমন 50 partitions
  compression: CompressionTypes.LZ4,     // batch compress → কম network, বেশি throughput
  messages: batchOfClicks.map(c => ({ key: c.userId, value: JSON.stringify(c) })),
});

// replay — নতুন model: পুরনো offset থেকে আবার পড়া
// kafka-consumer-groups --reset-offsets --to-datetime 2026-04-01T00:00:00 \
//   --group ml-training --topic clicks --execute
```

#### 🔍 আরও গভীরে
- **Replay** Kafka-র superpower — RabbitMQ-তে অসম্ভব (মেসেজ মুছে যায়)। retention ৩ মাস রাখলে যেকোনো সময় পুরনো ডেটা পুনরায় প্রসেস করা যায়।
- `fromBeginning: true` বা `--reset-offsets` দিয়ে নতুন group পুরো history পড়তে পারে।

### সমস্যা ৫: Database changes অন্য সিস্টেমে নিতে হবে (CDC)

**সমস্যা**: Order DB-তে যা বদলায়, তা search index, cache, data warehouse-এ sync করতে হবে — dual-write করলে inconsistency হয়।

**সমাধান**: **Change Data Capture** — Debezium (Kafka Connect) DB-র transaction log পড়ে প্রতিটা row change একটা topic-এ পাঠায়; consumer রা সেখান থেকে পড়ে।

```js
// Debezium যে event পাঠায় (Kafka Connect source connector, কোড লিখতে হয় না)
// topic: dbserver.shop.orders  →  { before, after, op: 'u'|'c'|'d', ts_ms }
await consumer.subscribe({ topic: 'dbserver.shop.orders' });
await consumer.run({ eachMessage: async ({ message }) => {
  const change = JSON.parse(message.value.toString());
  if (change.op === 'd') await searchIndex.remove(change.before.id);
  else await searchIndex.upsert(change.after);      // create/update
}});
```

#### 🔍 আরও গভীরে
- **Outbox pattern**-এরও ভিত্তি: DB transaction-এ একটা `outbox` টেবিলে event লিখুন, Debezium সেটা Kafka-তে তোলে — dual-write সমস্যা দূর হয়, atomicity ঠিক থাকে।
- compacted topic হলে প্রতিটা row-র current state সবসময় পাওয়া যায়।

### সমস্যা ৬: হাজার সার্ভারের লগ এক জায়গায় (Log Aggregation)

**সমস্যা**: শত শত microservice-এর লগ ছড়িয়ে আছে, খুঁজে debug করা কঠিন।

**সমাধান**: প্রতিটা সার্ভিস `logs` topic-এ লগ পাঠায়; একটা consumer সব Elasticsearch/S3-এ sink করে (Kafka Connect sink)।

```js
// অ্যাপ থেকে structured log পাঠানো (fire-and-forget, acks=0 → দ্রুত, কিছু হারালেও ঠিক আছে)
await producer.send({ topic: 'logs', acks: 0, messages: [{
  value: JSON.stringify({ svc: 'checkout', level: 'error', msg, ts: Date.now() }),
}]});
// consumer/Connect: logs → Elasticsearch, একই ডেটা → S3 (cold storage)
```

#### 🔍 আরও গভীরে
- লগের জন্য `acks=0` ঠিক আছে — throughput > একটা-দুটো লগ হারানো। Payment-এ কখনো নয়।
- একই `logs` topic থেকে real-time alerting আর long-term archival — আলাদা দুই consumer group।

### সমস্যা ৭: Notification fan-out — এক event, অনেক channel

**সমস্যা**: `order_shipped` হলে push, email, SMS — তিন channel-এ notification যাবে, কিন্তু একটা channel slow হলে বাকিরা আটকাবে না।

**সমাধান**: এক `notifications` topic, তিন consumer group (push/email/sms) — প্রতিটা নিজের গতিতে পড়ে।

```js
for (const ch of ['push', 'email', 'sms']) {
  const c = kafka.consumer({ groupId: `notify-${ch}` });    // আলাদা group
  await c.subscribe({ topic: 'notifications' });
  await c.run({ eachMessage: async ({ message }) =>
    sendVia(ch, JSON.parse(message.value.toString())) });   // এক channel slow → শুধু তার lag বাড়ে
}
```

### সমস্যা ৮: Third-party API rate-limit — consume গতি নিয়ন্ত্রণ

**সমস্যা**: একটা consumer external API-তে কল করে যেটা সেকেন্ডে ১০০ request-এর বেশি নেয় না; বেশি partition/consumer দিলে rate-limit ভাঙবে।

**সমাধান**: partition/consumer সংখ্যা সীমিত রাখুন (parallelism = partition), আর consumer-এ throttle/backoff দিন। Kafka **pull-based** বলে consumer নিজের গতিতে poll করে — natural backpressure।

```js
await consumer.run({ eachMessage: async ({ message }) => {
  await limiter.acquire();                 // token-bucket: 100/sec
  try { await callThirdParty(message); }
  catch (e) {
    if (e.retryable) { await sleep(backoff()); throw e; }  // throw → offset commit হবে না → আবার পড়বে
    else await toDeadLetter(message);                      // permanent fail → DLT
  }
}});
```

#### 🔍 আরও গভীরে
- Kafka-তে "মেসেজ ধরে রাখা" মানে শুধু offset commit না করা — মেসেজ log-এ থেকেই যায়, তাই RabbitMQ-র nack/requeue-র মতো আলাদা কিছু লাগে না।
- **Pause/Resume**: `consumer.pause([{topic, partitions}])` দিয়ে সাময়িকভাবে একটা partition-এর poll থামানো যায় (downstream down থাকলে)।

### সমস্যা ৯: Poison message আটকে দিচ্ছে — Dead Letter Topic (DLT)

**সমস্যা**: একটা malformed event বারবার fail করছে; offset commit না করায় consumer সেই একই মেসেজে আটকে গেছে, পুরো partition থেমে আছে।

**সমাধান**: কয়েকবার retry-র পরও fail করলে মেসেজটা একটা **Dead Letter Topic**-এ পাঠিয়ে offset এগিয়ে দিন — partition আর আটকাবে না।

```js
await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
  const retries = Number(message.headers?.['x-retry'] || 0);
  try {
    await process(message);
  } catch (e) {
    if (retries >= 3) {
      await producer.send({ topic: 'orders.DLT', messages: [{      // DLT-তে সরাও
        key: message.key, value: message.value,
        headers: { ...message.headers, 'x-error': e.message } }] });
    } else {
      await producer.send({ topic, messages: [{ key: message.key,  // retry counter বাড়িয়ে আবার
        value: message.value, headers: { ...message.headers, 'x-retry': String(retries + 1) } }] });
    }
  }
  await consumer.commitOffsets([{ topic, partition,
    offset: (Number(message.offset) + 1).toString() }]);   // যাই হোক, এগিয়ে যাও
}});
```

#### 🔍 আরও গভীরে
- Kafka-তে RabbitMQ-র মতো built-in DLQ নেই — **নিজে DLT topic বানাতে হয়** (এটা RabbitMQ-র সুবিধা)।
- retry topic pattern: `orders.retry.5s`, `orders.retry.30s` — delay-সহ retry-র জন্য আলাদা topic (Kafka-তে per-message delay নেই)।

---

[⬅ 10-Kafka-vs-RabbitMQ.md](./10-Kafka-vs-RabbitMQ.md) | [12-Real-World-Projects-Part2.md ➡](./12-Real-World-Projects-Part2.md) | [🏠 Repo Home](../README.md)
