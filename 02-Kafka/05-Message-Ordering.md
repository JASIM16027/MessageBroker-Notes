# Message Ordering — বিস্তারিত


Kafka-র ordering guarantee টা RabbitMQ থেকে আলাদা, আর এটা ইন্টারভিউ-এর খুব common প্রশ্ন। মূল কথা এক লাইনে: **Kafka শুধু একটা partition-এর ভেতরে order guarantee দেয়, পুরো topic-এ নয়।**

### ৫.১ কেন Kafka শুধু Partition-এর ভেতরে Order দেয়

একটা topic-এর একাধিক partition আছে, আর প্রতিটা partition আলাদা log — এদের মধ্যে কোনো global clock নেই। ধরুন `orders` topic-এর ৩টা partition, আর producer key ছাড়া মেসেজ পাঠাচ্ছে:

- `order_created` → P0-তে গেলো
- `order_paid` → P2-তে গেলো
- `order_shipped` → P1-তে গেলো

তিনটা আলাদা partition-এ, তিনজন আলাদা consumer পড়ছে — কে আগে process করবে তার কোনো guarantee নেই। ফলে `order_shipped` হয়তো `order_paid`-এর আগেই process হয়ে যেতে পারে। **একই order-এর event ভিন্ন partition-এ ছড়িয়ে গেলে order ভেঙে যায়।**

### ৫.২ সমাধান: Partition Key দিয়ে Same-Entity = Same-Partition

সমাধান সহজ — একই entity-র সব event **একই key** দিয়ে পাঠান, তাহলে সব একই partition-এ যাবে:

```js
// একই orderId = key → hash(key) % partitions একই → সবসময় একই partition
await producer.send({
  topic: 'orders',
  messages: [
    { key: order.id, value: JSON.stringify({ type: 'order_created', ...order }) },
    { key: order.id, value: JSON.stringify({ type: 'order_paid', ...order }) },
    { key: order.id, value: JSON.stringify({ type: 'order_shipped', ...order }) },
  ],
});
// তিনটাই একই partition-এ, একই order-এ — একজন consumer ঠিক ক্রমে পড়বে
```

- `hash(orderId) % numPartitions` সবসময় একই partition দেবে
- সেই partition group-এর একজন consumer-ই পড়ে, তাই order নিশ্চিত
- ভিন্ন order ভিন্ন partition-এ যেতে পারে — parallel-এ চলবে, তাতে সমস্যা নেই

### ৫.৩ Ordering আর Parallelism-এর Trade-off

এটাই মূল কৌশল — RabbitMQ-র "same account = same queue"-এর ঠিক Kafka version:

- **প্রতিটা entity-র ভেতরে**: order ঠিক (একই key → একই partition → একজন consumer)
- **overall system-এ**: অনেক partition parallel-এ চলছে, throughput ভালো

**সতর্কতা ১**: retry থাকলে ordering নষ্ট হতে পারে। Producer-এ `max.in.flight.requests.per.connection > 1` থাকলে একটা batch fail করে retry হওয়ার সময় পরের batch আগে চলে যেতে পারে। সমাধান: **`enable.idempotence=true`** (এটা থাকলে Kafka in-flight থাকা সত্ত্বেও order ঠিক রাখে) — এখন default।

**সতর্কতা ২**: partition সংখ্যা বদলালে `hash(key) % numPartitions` বদলে যায়, তাই পুরনো key নতুন partition-এ যেতে পারে — historically-ordered ডেটার ক্ষেত্রে সাবধান।

**মূল কথা**: RabbitMQ-র মতোই — global ordering দরকার হয় না, দরকার হয় **per-entity ordering**। Kafka-তে সেটা partition key দিয়েই স্বাভাবিকভাবে পাওয়া যায়, তাই Kafka high-throughput ordered stream-এর জন্য চমৎকার।

---

---

[⬅ 04-Internals.md](./04-Internals.md) | [06-Consumer-Group-Rebalancing.md ➡](./06-Consumer-Group-Rebalancing.md) | [🏠 Repo Home](../README.md)
