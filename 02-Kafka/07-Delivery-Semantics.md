# Delivery Semantics — Guarantee


"মেসেজ ঠিক কতবার deliver হবে" — এটা Kafka-তে configuration দিয়ে ঠিক করা যায়। তিনটা level:

### ৭.১ At-most-once, At-least-once, Exactly-once

| Guarantee | মানে | কীভাবে হয় | ঝুঁকি |
|---|---|---|---|
| **At-most-once** | ০ বা ১ বার | process-এর **আগে** offset commit | মেসেজ হারাতে পারে |
| **At-least-once** | ১ বা তার বেশি | process-এর **পরে** offset commit | duplicate হতে পারে |
| **Exactly-once** | ঠিক ১ বার | idempotence + transaction | জটিলতা/latency একটু বাড়ে |

বেশিরভাগ production সিস্টেম **at-least-once + idempotent consumer** ব্যবহার করে — মানে duplicate আসতে পারে ধরে নিয়ে consumer-কে idempotent বানানো হয় (একই মেসেজ দুবার এলেও ফল একই)।

### ৭.২ Idempotent Producer

`enable.idempotence=true` করলে producer retry-র কারণে সৃষ্ট duplicate ঠেকায়:
- প্রতিটা producer-কে একটা **PID (Producer ID)** দেওয়া হয়
- প্রতিটা record-এ partition-scoped **sequence number** থাকে
- Broker duplicate/out-of-order sequence ধরে ফেলে — একই record দুবার লেখে না, order-ও ঠিক রাখে

এটা এখন Kafka-তে **default on** (`acks=all`, retries সহ)। **সীমা**: এটা শুধু একটা producer session-এর একটা partition-এ duplicate ঠেকায়, cross-partition বা application-level duplicate নয় — তার জন্য transaction বা idempotent consumer লাগে।

### ৭.৩ Transactions (Read-Process-Write, Exactly-once)

সত্যিকারের exactly-once দরকার হয় "read from Kafka → process → write to Kafka" প্যাটার্নে (যেমন Kafka Streams)। Transaction দিয়ে **consume + produce + offset commit** — সব একসাথে atomic হয়:

```js
const producer = kafka.producer({
  transactionalId: 'payment-processor-1',      // stable id দরকার
  idempotent: true, maxInFlightRequests: 1,
});
await producer.connect();

const txn = await producer.transaction();
try {
  await txn.send({ topic: 'payments.settled', messages: [{ key: id, value: out }] });
  // input offset-ও একই transaction-এ commit — process আর commit atomic
  await txn.sendOffsets({
    consumerGroupId: 'payment-processor',
    topics: [{ topic: 'payments', partitions: [{ partition, offset }] }],
  });
  await txn.commit();          // হয় সব হবে, নয় কিছুই না
} catch (e) {
  await txn.abort();
}
```

Consumer পাশে `isolation.level=read_committed` সেট করলে শুধু committed transaction-এর মেসেজ পড়বে (aborted গুলো skip করবে)। এভাবেই end-to-end exactly-once পাওয়া যায় — তবে latency ও জটিলতা বাড়ে, তাই সত্যিই দরকার না হলে at-least-once + idempotent consumer-ই সহজ।

---

---

[⬅ 06-Consumer-Group-Rebalancing.md](./06-Consumer-Group-Rebalancing.md) | [08-High-Availability-Replication-ISR-KRaft.md ➡](./08-High-Availability-Replication-ISR-KRaft.md) | [🏠 Repo Home](../README.md)
