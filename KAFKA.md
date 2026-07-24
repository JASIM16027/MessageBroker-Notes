# 📗 Apache Kafka মাস্টার গাইড

> Kafka হচ্ছে একটা **distributed event streaming platform** — মানে বিশাল পরিমাণ ডেটা (event) কে একটা **append-only log**-এ জমা করে, একাধিক সিস্টেমের মধ্যে real-time-এ পাঠানো-আনানো ও পরে replay করার কাজ করে। RabbitMQ যেখানে "task queue", Kafka সেখানে "event log"।

> 📘 এই গাইডটা [RabbitMQ মাস্টার গাইড](./README.md)-এর সঙ্গী। দুটো একসাথে পড়লে message broker vs event streaming — পুরো ছবিটা পরিষ্কার হয়ে যাবে।

## 🎯 শেখার রোডম্যাপ — কোনটার পর কোনটা শিখবেন

সবচেয়ে কার্যকর ক্রম: **আগে বেসিক ধারণা → তারপর core mechanics (partition/offset) → তারপর consumer group → তারপর delivery guarantee → তারপর production reliability → তারপর comparison → তারপর হাতে-কলমে project → সবশেষে interview revision।** নিচের ধাপগুলো ঠিক এই ক্রমে অনুসরণ করুন।

| ধাপ | Phase | কী শিখবেন | সেকশন |
|---|---|---|---|
| ১ | **Basics** (Beginner) | Kafka কী, কেন, আর মূল flow | 1 → 2 → 3 |
| ২ | **Core Mechanics** | Topic, Partition, Offset, Broker — ভেতরের কম্পোনেন্ট | 4 → 5 |
| ৩ | **Intermediate** | Consumer Group, Rebalancing, Delivery Semantics | 6 → 7 |
| ৪ | **Production & Reliability** | Replication, ISR, KRaft, Retention/Compaction | 8 → 9 |
| ৫ | **Decision / Comparison** | Kafka কখন, RabbitMQ কখন | 10 |
| ৬ | **হাতে-কলমে Practice** | ১৬টি বাস্তব project সমস্যা কোডসহ | 11 |
| ৭ | **Interview Revision** | সব লেভেলের প্রশ্ন + Q1–Q30 | 12 → 13 |

> **টিপস**: প্রথমবার পড়ার সময় ধাপ ১–৫ ভালোভাবে বুঝে তারপর ধাপ ৬-এ (Real-World Projects) নিজে কোড লিখে দেখুন। ধাপ ৭ (Interview) সবশেষে revision হিসেবে রাখুন। Kafka-তে **সবকিছুর মূলে Partition আর Offset** — এই দুটো ভালো করে বুঝলে বাকিটা সহজ।

---

## 📋 বিস্তারিত সূচিপত্র

## ১. Kafka পরিচিতি ও বাস্তব উদাহরণ

**আলোচ্য বিষয়:**

* Kafka কী এবং কেন — মূল ধারণা
* Uber/Pathao (রিয়েল-টাইম লোকেশন স্ট্রিম)
* Netflix/YouTube (ভিউয়িং clickstream)
* Payment/Fraud detection পাইপলাইন
* IoT / Sensor telemetry
* Log aggregation (সব সার্ভারের লগ এক জায়গায়)

---

## ২. কেন Kafka — যে সমস্যা সমাধান করে

**আলোচ্য বিষয়:**

* Point-to-point integration-এর "spaghetti" সমস্যা
* Log হিসেবে central nervous system
* একই ডেটা multiple consumer পড়া + replay

---

## ৩. কার্যপ্রণালী — Flow (৬ ধাপ)

**আলোচ্য বিষয়:**

* Producer
* Topic
* Partition
* Broker
* Consumer / Consumer Group
* Offset ও Commit

---

## ৪. Internals — ভেতরের মেকানিজম

**আলোচ্য বিষয়:**

* Topic ও Partition — কেন partition-ই স্কেলিং-এর চাবি
* Offset — প্রতিটা মেসেজের ঠিকানা
* Partition Key — কোন মেসেজ কোন partition-এ
* Segment, Log ও কীভাবে disk-এ থাকে
* Replication, Leader/Follower, ISR
* Controller (KRaft vs ZooKeeper)
* Producer acks, retries, idempotence

---

## ৫. Message Ordering (order guarantee)

**আলোচ্য বিষয়:**

* কেন Kafka শুধু partition-এর ভেতরে order দেয়
* Partition Key দিয়ে same-entity = same-partition
* Ordering আর parallelism-এর trade-off

---

## ৬. Consumer Group ও Rebalancing

**আলোচ্য বিষয়:**

* Consumer Group কী — load ভাগ কীভাবে হয়
* Partition assignment ও Rebalance
* Offset commit — auto vs manual
* Sticky / Cooperative rebalance, Lag

---

## ৭. Delivery Semantics (Guarantee)

**আলোচ্য বিষয়:**

* At-most-once, At-least-once, Exactly-once
* Idempotent Producer
* Transactions (read-process-write)

---

## ৮. High Availability — Replication, ISR, KRaft

**আলোচ্য বিষয়:**

* Replication factor ও Leader/Follower
* ISR (In-Sync Replicas) ও `min.insync.replicas`
* `acks=all` — কেন durability-র চাবি
* Broker fail হলে কী হয় (leader election)
* ZooKeeper → KRaft transition

---

## ৯. Retention, Compaction ও Storage

**আলোচ্য বিষয়:**

* Time / Size based retention
* Log Compaction — কী, কখন
* Tiered Storage

---

## ১০. Kafka বনাম RabbitMQ

**আলোচ্য বিষয়:**

* মূল আর্কিটেকচারাল পার্থক্য (Log vs Queue)
* কেন এই পার্থক্য Use Case নির্ধারণ করে
* Practical Comparison Table
* একই কোম্পানি দুটোই ব্যবহার করে

---

## ১১. Real-World Projects — ১৬টি কেস (কোডসহ)

**আলোচ্য বিষয়:** Order pipeline, Fraud detection, Clickstream, IoT, CDC, Log aggregation, Notification fan-out, Rate-limit, Dedup/Idempotency, DLT, Reprocessing/Replay, Multi-datacenter ইত্যাদি।

---

## ১২. ইন্টারভিউ প্রশ্ন — সব লেভেল

## ১৩. Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q30)

---


## ১. Kafka কী এবং কেন — Real-life Examples

Kafka হচ্ছে একটা **distributed event streaming platform** — মূলত একটা বিশাল, বণ্টিত (distributed), append-only **log**। Producer রা log-এর শেষে event লিখে যায়, আর Consumer রা নিজের গতিতে সেই log পড়ে। RabbitMQ-তে মেসেজ consume হলে মুছে যায়; Kafka-তে event পড়ার পরও log-এ থেকে যায় (retention period পর্যন্ত), তাই একই ডেটা অনেকে পড়তে পারে ও পরে replay করা যায়। নিচে কিছু real life example:

### ১. Ride-Sharing App (Uber/Pathao) — রিয়েল-টাইম লোকেশন স্ট্রিম
- প্রতিটা ড্রাইভারের ফোন প্রতি সেকেন্ডে GPS লোকেশন পাঠায় → লক্ষ লক্ষ event/সেকেন্ড
- সব event একটা `driver.location` topic-এ যায়
- একই স্ট্রিম আলাদা আলাদা টিম পড়ে: ETA calculation, nearby-driver matching, surge pricing, map display — কেউ কারো পড়া আটকায় না

**ফায়দা**: বিশাল throughput সামলায়, আর একই ডেটা একাধিক consumer independently পড়তে পারে।

### ২. Netflix/YouTube — ভিউয়িং Clickstream
- ইউজার কী দেখলো, কতক্ষণ দেখলো, কোথায় pause করলো — প্রতিটা এই event Kafka-তে যায়
- Recommendation engine, Analytics, A/B testing, Billing — সবাই একই event log আলাদাভাবে consume করে
- নতুন recommendation model বানাতে গত ৩ মাসের event **replay** করা যায়

### ৩. Payment / Fraud Detection Pipeline
- প্রতিটা transaction event `transactions` topic-এ যায়
- Fraud detection service real-time-এ পড়ে সন্দেহজনক প্যাটার্ন ধরে
- একই event ledger service, notification service, analytics — সবাই পড়ে

### ৪. IoT / Sensor Telemetry
- লক্ষ লক্ষ sensor (temperature, pressure, energy meter) থেকে প্রতি সেকেন্ডে ডেটা আসে
- Kafka এই high-volume ingestion সামলায়, তারপর stream processing (Kafka Streams / Flink) দিয়ে real-time aggregation হয়

### ৫. Log Aggregation
- হাজার হাজার সার্ভার/microservice-এর application log এক জায়গায় (একটা topic-এ) জমা হয়
- সেখান থেকে Elasticsearch/S3-এ যায়, monitoring ও search-এর জন্য

### মূল ধারণাটা কী?
সব উদাহরণেই একটা common pattern: **একটা event একবার log-এ লেখা হয়, আর অনেক ভিন্ন consumer সেটা নিজের গতিতে, নিজের মতো করে পড়ে — কেউ পড়লেও ডেটা মোছে না।** এতে:
- বিশাল throughput (millions/sec) সামলানো যায়
- একই ডেটা multiple team শেয়ার করে (duplicate storage ছাড়াই)
- পুরনো ডেটা replay করা যায় (bug fix বা নতুন model-এর জন্য)
- সিস্টেমগুলো decoupled থাকে — একটা "central nervous system"

---


## ২. যে সমস্যা সমাধান করে

Kafka কীভাবে কাজ করে এবং কী সমস্যা সমাধান করে, ধাপে ধাপে বুঝিয়ে দিচ্ছি।

ধরুন আপনার একটা বড় কোম্পানি — যেখানে অনেকগুলো সিস্টেম একে অপরের ডেটা চায়:

- **Order Service**-এর ডেটা লাগে: Inventory, Shipping, Analytics, Recommendation, Fraud, Email — ৬টা সিস্টেমের
- **User activity** ডেটা লাগে: Analytics, Personalization, Ad-targeting — আরও কয়েকটার

**Kafka ছাড়া (point-to-point integration)** — প্রতিটা সিস্টেমকে প্রতিটা অন্য সিস্টেমের সাথে সরাসরি জুড়তে হয়। ৬টা সিস্টেম হলে সম্ভাব্য সংযোগ প্রায় N×N — এটাকে বলে **"integration spaghetti"**:
- নতুন একটা consumer যোগ করতে হলে source service-এর কোড বদলাতে হয়
- একটা downstream slow/down হলে upstream আটকে যায়
- একই ডেটা বারবার আলাদা করে পাঠাতে হয়, কেউ পুরনো ডেটা পেতে পারে না

**Kafka দিয়ে** — Order Service শুধু একটাবার `orders` topic-এ event লেখে। যত সিস্টেম দরকার, সবাই সেই topic subscribe করে নিজের গতিতে পড়ে। Source-কে জানতেও হয় না কে কে পড়ছে। নতুন consumer যোগ করা মানে শুধু নতুন একটা consumer group — source-এ কোনো পরিবর্তন লাগে না। এটাই Kafka-র মূল শক্তি: **producer আর consumer সম্পূর্ণ decoupled, আর ডেটা log-এ থাকে বলে যেকোনো সময়, যতবার খুশি পড়া যায়।**

---


## ৩. কীভাবে কাজ করে — Flow

Kafka-র পুরো flow টা ধাপে ধাপে বুঝি:

### ১. Producer
আপনার অ্যাপ্লিকেশন একটা **event** (record) তৈরি করে Kafka-তে পাঠায়। প্রতিটা record-এ থাকে একটা optional **key**, একটা **value**, আর কিছু metadata। Producer response wait না করেও পাঠাতে পারে (fire-and-forget), আবার confirmation (`acks`)-ও চাইতে পারে।

### ২. Topic
Event গুলো একটা **Topic**-এ যায় — এটা একটা named "category" বা "feed", যেমন `orders`, `payments`, `driver.location`। RabbitMQ-র queue-র সাথে তুলনা করলে topic অনেকটা "log ফাইলের নাম", কিন্তু বড় পার্থক্য: consume করলে মেসেজ মোছে না।

### ৩. Partition
এটাই Kafka-র মূল ধারণা। প্রতিটা Topic কে একাধিক **Partition**-এ ভাগ করা হয় (যেমন `orders`-এর ৬টা partition)। প্রতিটা partition একটা আলাদা, ordered, append-only log। **Partition-ই scaling আর ordering দুটোরই ভিত্তি** — একাধিক partition মানে একাধিক consumer parallel-এ পড়তে পারে, আর প্রতিটা partition-এর ভেতরে মেসেজ order ঠিক থাকে।

### ৪. Broker
Kafka **Broker** হলো একটা সার্ভার যেটা partition-গুলো store করে ও serve করে। কয়েকটা broker মিলে একটা **Cluster**। একটা topic-এর partition-গুলো বিভিন্ন broker-এ ছড়িয়ে থাকে, তাই load ভাগ হয়ে যায় এবং একটা broker down হলেও (replication থাকলে) ডেটা বাঁচে।

### ৫. Consumer / Consumer Group
একটা **Consumer** partition থেকে event পড়ে। একাধিক consumer মিলে একটা **Consumer Group** বানায় — Kafka topic-এর partition-গুলো group-এর consumer-দের মধ্যে ভাগ করে দেয় (একটা partition একসাথে group-এর একজনই পড়ে)। ফলে load parallel-এ ভাগ হয়। ভিন্ন consumer group একই topic সম্পূর্ণ independently পড়তে পারে।

### ৬. Offset ও Commit
প্রতিটা partition-এ প্রতিটা event-এর একটা ক্রমিক নাম্বার আছে — **offset** (0, 1, 2, 3…)। Consumer ট্র্যাক রাখে সে কোন offset পর্যন্ত পড়েছে, আর সেটা Kafka-তে **commit** করে (`__consumer_offsets` নামের internal topic-এ)। Consumer crash করে আবার উঠলে, শেষ committed offset থেকে আবার শুরু করে — তাই মেসেজ হারায় না।

**মূল কথা**: RabbitMQ broker ঠিক করে "কে কোন মেসেজ পাবে আর কখন মুছবে" (smart broker)। Kafka broker শুধু log রাখে; **consumer নিজে ট্র্যাক রাখে সে কোথায় আছে** (smart consumer)। এই একটা পার্থক্যই replay, multiple-reader — সব সম্ভব করে।

---


## ৪. ভেতরের মেকানিজম — Internals

আরও গভীরে যাই, একটা একটা করে Kafka-র ভেতরের মেকানিজম বুঝিয়ে দিচ্ছি।

### Topic ও Partition — কেন Partition-ই স্কেলিং-এর চাবি

একটা Topic হলো logical নাম; আসল ডেটা থাকে **Partition**-এ। ধরুন `orders` topic-এর ৪টা partition (P0, P1, P2, P3):

- প্রতিটা partition একটা স্বাধীন **ordered log** — নিজের offset sequence আছে
- একটা partition সবসময় **একটা broker**-এ leader হিসেবে থাকে (replica অন্য broker-এ)
- Consumer group-এ ৪টা consumer থাকলে প্রতিজন একটা করে partition পাবে → 4x parallelism
- Consumer সংখ্যা partition সংখ্যার বেশি হলে বাড়তি consumer **idle** বসে থাকবে — তাই **partition সংখ্যাই maximum parallelism নির্ধারণ করে**

**গুরুত্বপূর্ণ**: partition বাড়ানো যায় কিন্তু কমানো যায় না, আর partition বাড়ালে key→partition mapping বদলে যায় (ordering-এ প্রভাব পড়ে) — তাই শুরুতেই একটু বেশি partition রাখা common practice।

### Offset — প্রতিটা মেসেজের ঠিকানা

Offset হলো একটা partition-এর ভেতরে একটা event-এর position (0 থেকে শুরু)। এটা:
- **Partition-scoped** — P0-এর offset 5 আর P1-এর offset 5 সম্পূর্ণ আলাদা মেসেজ
- **Immutable ও ever-increasing** — একবার assign হলে বদলায় না
- Consumer-এর "bookmark" — কোন offset পর্যন্ত পড়া হয়েছে সেটাই commit হয়

তিন ধরনের offset মাথায় রাখুন: **log-start offset** (retention-এর পর পুরনো অংশ মুছলে এটা বাড়ে), **committed offset** (consumer কতদূর confirm করেছে), **log-end offset (LEO)** (সর্বশেষ লেখা মেসেজের পরের position)। **Consumer Lag = LEO − committed offset** — consumer কতটা পিছিয়ে আছে, monitoring-এর সবচেয়ে গুরুত্বপূর্ণ metric।

### Partition Key — কোন মেসেজ কোন Partition-এ

Producer প্রতিটা record-এ একটা **key** দিতে পারে। Kafka ঠিক করে record কোন partition-এ যাবে এভাবে:
- **Key থাকলে**: `partition = hash(key) % numPartitions` → **একই key সবসময় একই partition-এ** যায়
- **Key না থাকলে**: sticky/round-robin ভাবে partition-এ ছড়িয়ে দেয় (load balance, কিন্তু ordering guarantee নেই)

এই key-mechanism-ই ordering-এর চাবি: `orderId` বা `accountId` কে key বানালে সেই entity-র সব event একই partition-এ, একই order-এ থাকবে (সেকশন ৫ দ্রষ্টব্য)।

### Segment, Log ও কীভাবে Disk-এ থাকে

প্রতিটা partition disk-এ একগুচ্ছ **segment ফাইল** হিসেবে থাকে (যেমন প্রতি ১GB বা প্রতি সপ্তাহে নতুন segment)। Kafka এত দ্রুত কেন:
- **Sequential disk write** — log-এর শেষে append, random write নয় (disk-এ sequential অনেক দ্রুত)
- **Zero-copy** — disk থেকে network-এ সরাসরি পাঠায়, application memory-তে copy না করে (`sendfile`)
- **Page cache** — OS-এর page cache ব্যবহার করে, তাই recent ডেটা প্রায় RAM-speed-এ পড়া যায়
- **Batching + compression** — producer অনেক record একসাথে batch করে, compress করে পাঠায়

পুরনো segment retention policy অনুযায়ী পুরোটা মুছে ফেলা হয় (পুরো ফাইল delete — খুব সস্তা)।

### Replication, Leader/Follower, ISR

প্রতিটা partition-এর কয়েকটা **replica** থাকে (replication factor, যেমন ৩)। এদের মধ্যে একটা **Leader**, বাকিরা **Follower**:
- সব read/write **leader**-এর মাধ্যমে হয়
- Follower রা leader থেকে ডেটা fetch করে নিজেকে sync রাখে
- যেসব follower যথেষ্ট up-to-date, তারা **ISR (In-Sync Replicas)** সেটে থাকে
- Leader fail করলে **ISR থেকে একটা follower নতুন leader** হয় → কোনো committed ডেটা হারায় না

(বিস্তারিত সেকশন ৮-এ।)

### Controller — ZooKeeper বনাম KRaft

Cluster-এ একটা **Controller** থাকে যে partition leadership, replica assignment এসব ম্যানেজ করে:
- **পুরনো**: cluster metadata ও controller election **ZooKeeper** নামের আলাদা সিস্টেম সামলাতো
- **নতুন (KRaft mode)**: Kafka নিজেই Raft consensus দিয়ে metadata সামলায়, ZooKeeper আর লাগে না — সহজ deployment, দ্রুত failover, বেশি scalable (Kafka 3.3+ production-ready, 4.0-এ ZooKeeper বাদ)

### Producer acks, Retries, Idempotence

Producer-এর কয়েকটা গুরুত্বপূর্ণ সেটিং যা reliability ঠিক করে:
- **`acks=0`**: leader-এর confirmation-ও চায় না — দ্রুততম, কিন্তু মেসেজ হারাতে পারে
- **`acks=1`**: leader লিখলেই confirm — leader fail করলে (follower-এ পৌঁছানোর আগে) হারাতে পারে
- **`acks=all`**: ISR-এর সব replica লিখলে তবে confirm — সবচেয়ে নিরাপদ
- **`enable.idempotence=true`**: retry-র কারণে duplicate লেখা ঠেকায় (প্রতিটা producer-record-এ sequence number দিয়ে broker duplicate ধরে) — এটাই এখন default (সেকশন ৭)

---


## ৫. Message Ordering — বিস্তারিত

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


## ৬. Consumer Group ও Rebalancing

Consumer group Kafka-র সবচেয়ে গুরুত্বপূর্ণ scaling ধারণা — ভালো করে বুঝি।

### ৬.১ Consumer Group কী — Load ভাগ কীভাবে হয়

একটা **Consumer Group** হলো এক নামে (`group.id`) চলা কয়েকটা consumer, যারা মিলে একটা topic পড়ে। Kafka topic-এর partition-গুলো group-এর member-দের মধ্যে ভাগ করে দেয়:

- ৬ partition, ৩ consumer → প্রতিজন ২টা partition পায়
- ৬ partition, ৬ consumer → প্রতিজন ১টা (সর্বোচ্চ parallelism)
- ৬ partition, ৮ consumer → ৬ জন কাজ করে, ২ জন idle (partition-ই limit)

**মূল নিয়ম**: একটা partition একসাথে group-এর **একজনই** পড়ে (তাই ordering ঠিক থাকে)। কিন্তু **ভিন্ন consumer group একই partition independently** পড়তে পারে — এভাবেই একই ডেটা analytics, fraud, billing সবাই আলাদাভাবে পড়ে।

```js
// দুই আলাদা group একই topic পড়ছে — একে অপরকে প্রভাবিত করে না
const analytics = kafka.consumer({ groupId: 'analytics-group' });
const fraud     = kafka.consumer({ groupId: 'fraud-group' });
// প্রতিটা group নিজের offset আলাদা করে ট্র্যাক করে
```

### ৬.২ Rebalancing — Partition Assignment বদলানো

যখন group-এ consumer যোগ হয়/চলে যায় (crash, deploy, scale), Kafka partition-গুলো আবার ভাগ করে — একে **Rebalance** বলে:

1. একজন consumer মারা গেলে তার partition-গুলো বাকিদের মধ্যে ভাগ হয়ে যায়
2. নতুন consumer যোগ হলে তাকে কিছু partition দেওয়া হয়
3. Rebalance চলাকালীন **সংক্ষিপ্ত সময়ের জন্য consumption থামে** ("stop-the-world") — এটাই rebalance-এর মূল খরচ

Rebalance trigger হয়: consumer heartbeat মিস করলে (`session.timeout.ms`), `max.poll.interval.ms`-এর মধ্যে poll না করলে (process করতে বেশি সময় লাগলে), বা partition সংখ্যা বদলালে।

### ৬.৩ Offset Commit — Auto vs Manual

Consumer কতদূর পড়েছে তা `__consumer_offsets` topic-এ commit করে:

- **Auto-commit** (`enable.auto.commit=true`): প্রতি `auto.commit.interval.ms` (default 5s) পরপর নিজে থেকে commit করে। সহজ, কিন্তু ঝুঁকি — commit হয়ে গেছে কিন্তু process শেষ হয়নি এমন সময় crash করলে **মেসেজ হারায়** (at-most-once ঝুঁকি), বা উল্টো duplicate।
- **Manual commit** (`enable.auto.commit=false`): process **শেষ করার পর** নিজে commit করেন — production-এ recommended। এটাই at-least-once নিশ্চিত করে।

```js
const consumer = kafka.consumer({ groupId: 'orders-worker', });
await consumer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: false });
await consumer.run({
  autoCommit: false,                          // manual — process শেষে commit
  eachMessage: async ({ topic, partition, message }) => {
    await handleOrder(JSON.parse(message.value.toString()));  // আগে কাজ
    await consumer.commitOffsets([{            // তারপর commit — at-least-once
      topic, partition, offset: (Number(message.offset) + 1).toString(),
    }]);
  },
});
```

> ⚠️ **নোট**: offset commit করার সময় `+1` করতে হয় — কারণ commit মানে "এই offset থেকে পরেরটা পড়া শুরু করবো", অর্থাৎ সর্বশেষ-পড়া offset + 1।

### ৬.৪ Sticky / Cooperative Rebalance ও Lag

- **Cooperative (Incremental) Rebalance**: পুরনো "stop-the-world" পদ্ধতিতে rebalance হলে সব partition ছেড়ে দিয়ে আবার নেওয়া হতো। Cooperative sticky assignor শুধু যেগুলো সরানো দরকার সেগুলো সরায় — downtime অনেক কম। নতুন version-এ এটাই recommended।
- **Consumer Lag**: `LEO − committed offset`। Lag বাড়তে থাকলে বুঝবেন consumer produce-এর গতি ধরতে পারছে না — তখন consumer/partition বাড়াতে হয়। এটাই production-এ সবচেয়ে গুরুত্বপূর্ণ alert।

---


## ৭. Delivery Semantics — Guarantee

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


## ৮. High Availability — Replication, ISR, KRaft

Kafka কীভাবে একটা broker down হলেও ডেটা না হারিয়ে চলতে থাকে, বিস্তারিত বুঝি।

### ৮.১ Replication Factor ও Leader/Follower

প্রতিটা partition-কে কয়েক copy-তে রাখা হয় — **replication factor** (production-এ সাধারণত ৩)। ৩ broker-এর cluster-এ replication factor ৩ মানে:
- প্রতিটা partition-এর ১টা **Leader** + ২টা **Follower**, তিনটা আলাদা broker-এ
- সব read/write leader-এ হয়; follower রা leader থেকে fetch করে sync থাকে
- একটা broker down হলে সেই broker-এ থাকা leader partition-গুলোর জন্য অন্য broker-এর follower নতুন leader হয়

### ৮.২ ISR ও `min.insync.replicas`

**ISR (In-Sync Replicas)** = যেসব replica leader-এর সাথে যথেষ্ট up-to-date (নির্দিষ্ট সময়ের মধ্যে sync করেছে)। Slow/dead follower ISR থেকে বাদ পড়ে, আবার catch up করলে ফিরে আসে।

- **`acks=all`**: leader তখনই producer-কে confirm করে যখন **ISR-এর সব replica** মেসেজ লিখেছে
- **`min.insync.replicas=2`**: অন্তত ২টা replica in-sync না থাকলে producer write **reject** করবে (`acks=all`-এর সাথে)

এই দুটো একসাথে **durability-র চাবি**: replication factor 3 + `min.insync.replicas=2` + `acks=all` মানে — অন্তত ২ জায়গায় ডেটা না লেখা পর্যন্ত write সফল ধরা হবে না, তাই ১টা broker হারালেও ডেটা বাঁচে। (একটা trade-off: ২টার কম replica in-sync থাকলে availability হারিয়ে write বন্ধ হবে — consistency-কে অগ্রাধিকার।)

### ৮.৩ Broker Fail হলে কী হয় (Leader Election)

1. একটা broker crash করলো → তার হাতে থাকা leader partition-গুলো unavailable
2. **Controller** বুঝতে পারে, আর ISR থেকে একটা up-to-date follower-কে নতুন leader বানায়
3. Producer/Consumer metadata refresh করে নতুন leader-এর সাথে কাজ চালিয়ে যায় (client-এ built-in retry)
4. যেহেতু নতুন leader ISR-এ ছিল (committed ডেটা তার কাছে আছে), **কোনো committed মেসেজ হারায় না**

> ⚠️ **Unclean leader election**: যদি ISR-এর কোনো replica না বাঁচে আর `unclean.leader.election.enable=true` থাকে, তাহলে out-of-sync replica leader হতে পারে — কিছু ডেটা হারানোর বিনিময়ে availability ফিরে আসে। Production-এ সাধারণত এটা `false` রাখা হয় (data safety আগে)।

### ৮.৪ ZooKeeper → KRaft

- **আগে**: cluster metadata (কোন partition কোথায়, ISR কারা, controller কে) **ZooKeeper**-এ থাকতো — আলাদা একটা সিস্টেম maintain করতে হতো, বড় cluster-এ metadata bottleneck হতো
- **এখন (KRaft)**: Kafka নিজেই একটা internal Raft-based metadata log দিয়ে সব সামলায়। ফায়দা: এক সিস্টেম (deploy সহজ), দ্রুত controller failover, লক্ষ লক্ষ partition scale করা যায়। Kafka 3.3+ থেকে production-ready, 4.0-তে ZooKeeper সম্পূর্ণ বাদ।

---


## ৯. Retention, Compaction ও Storage

Kafka-তে মেসেজ consume করলেই মোছে না — কতদিন থাকবে তা **retention policy** ঠিক করে। এটাই replay সম্ভব করে।

### ৯.১ Time / Size Based Retention

- **Time-based** (`retention.ms`, default ৭ দিন): এই সময়ের পুরনো segment মুছে যায়
- **Size-based** (`retention.bytes`): partition একটা নির্দিষ্ট সাইজ ছাড়ালে পুরনো segment মুছে যায়
- দুটোর যেকোনোটা আগে hit করলেই মোছা শুরু হয়

গুরুত্বপূর্ণ: consumer পড়ুক বা না পড়ুক, retention শেষ হলে ডেটা যাবে। তাই **consumer lag retention-এর চেয়ে বেশি হয়ে গেলে ডেটা হারানোর ঝুঁকি** — monitoring জরুরি।

### ৯.২ Log Compaction — কী, কখন

সাধারণ retention পুরো সময়ভিত্তিক। **Compaction** (`cleanup.policy=compact`) আলাদা — এটা প্রতিটা **key-এর শুধু সর্বশেষ value** রাখে, পুরনো গুলো মুছে দেয়:

- ব্যবহার: "current state" ধরনের ডেটা — যেমন `user_id → latest_profile`, বা database-এর changelog
- log যত বড়ই হোক, প্রতিটা key-র একটা করে সর্বশেষ record থাকবে
- একটা key মুছতে হলে সেই key-তে `null` value ("**tombstone**") পাঠানো হয়

Kafka Streams-এর KTable, `__consumer_offsets` — এগুলো compacted topic। Real-world: একটা compacted topic থেকে যেকোনো সময় পুরো current state rebuild করা যায়।

### ৯.৩ Tiered Storage

নতুন Kafka-তে (KIP-405) পুরনো segment স্থানীয় disk থেকে সস্তা **object storage (S3 ইত্যাদি)**-তে সরানো যায়, recent ডেটা local-এ থাকে। ফায়দা: broker-এর local disk ছোট রেখেও মাসের পর মাসের ডেটা সস্তায় ধরে রাখা যায় (long-term replay/compliance)।

---


## ১০. Kafka বনাম RabbitMQ

এটা সবচেয়ে common ইন্টারভিউ প্রশ্ন। মূল পার্থক্যটা architecture-এ, আর সেটাই use case ঠিক করে দেয়। (RabbitMQ-র দিক থেকে এই তুলনার আরও বিস্তারিত [README-এর সেকশন ৮](./README.md)-এ আছে।)

### মূল আর্কিটেকচারাল পার্থক্য

#### RabbitMQ: Queue-based (Smart Broker, Dumb Consumer)
মেসেজ একটা **task** — কেউ consume করে, ack দেয়, মেসেজ **মুছে যায়**। Broker routing, priority, retry, DLQ — সব জটিল লজিক সামলায়।

#### Kafka: Log-based (Dumb Broker, Smart Consumer)
Event একটা **append-only log**-এ যোগ হয়, পড়লেও **মোছে না** (retention পর্যন্ত)। Consumer নিজে offset ট্র্যাক করে; একই ডেটা multiple group পড়তে পারে ও replay করা যায়। Broker শুধু log store/serve করে।

### কেন এই পার্থক্য Use Case নির্ধারণ করে

**Kafka কেন Streaming/Analytics-এর জন্য ভালো:**
1. **একই ডেটা multiple team** independently পড়ে (consumer group), duplicate storage ছাড়াই
2. **বিশাল throughput** — sequential disk write + zero-copy + batching, তাই millions/sec
3. **Replay** — offset পিছিয়ে পুরনো ডেটা আবার প্রসেস (নতুন model, bug fix)
4. **Ordered high-throughput stream** — per-partition ordering + parallelism একসাথে

**RabbitMQ কেন Task/Command-এর জন্য ভালো:**
1. **Complex routing** (topic/fanout exchange, priority, per-message TTL) built-in
2. **Per-message DLQ, priority** — সহজ, Kafka-তে নিজে বানাতে হয়
3. একটা task একজন worker-ই নেবে — natural competing-consumer semantics
4. কম latency, ছোট scale-এ সহজ setup

### Practical Comparison Table

| বিষয় | Kafka | RabbitMQ |
|---|---|---|
| মূল model | Distributed log (event streaming) | Message queue (task broker) |
| মেসেজ delete হয় কখন | Retention শেষে (consume করলেও থাকে) | Ack-এর পর সাথে সাথে |
| Throughput | অনেক বেশি (লক্ষ-কোটি/সেকেন্ড) | মাঝারি (হাজার-লক্ষ/সেকেন্ড) |
| Replay | হ্যাঁ (offset দিয়ে) | না |
| একই ডেটা multi-consumer | Natural (consumer groups) | Fanout দিয়ে duplicate করতে হয় |
| Ordering | Per-partition guaranteed | Per-queue (single consumer হলে) |
| Priority / per-msg TTL | নিজে বানাতে হয় | Built-in |
| Routing | Partition key / topic | Exchange (direct/topic/fanout/headers) |
| Scaling unit | Partition | Queue / consumer |
| Consumer model | Pull (নিজে poll করে) | Push (broker পাঠায়) |

### Real World: একই কোম্পানি দুটোই ব্যবহার করে

একটা বড় e-commerce (Daraz টাইপ) সাধারণত **দুটোই** ব্যবহার করে:
- **Kafka**: clickstream, activity log, order-event stream — যেখানে বিশাল volume, multiple consumer, replay দরকার
- **RabbitMQ**: payment command, invoice generation, SMS পাঠানো — যেখানে task-based, priority/DLQ, exactly-once execution দরকার

**"একটাই কেন নয়?"** — Kafka দিয়ে priority+per-message-DLQ সহ complex task routing করতে অনেক বাড়তি কোড লাগে; RabbitMQ দিয়ে millions/sec replay-able stream সামলানো কঠিন। **Right tool for the right job।**

---


## ১১. Real-World Project — সমস্যা ও সমাধান (কোডসহ)

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


## ১২. ইন্টারভিউ প্রশ্ন — সব লেভেল

নিচে লেভেল অনুযায়ী প্রশ্নগুলো — আগে নিজে উত্তর ভাবুন, তারপর সেকশন ১৩-এর বিস্তারিত উত্তরের সাথে মিলিয়ে নিন।

### Basic Conceptual Questions
- Kafka কী? এটা কোন সমস্যা সমাধান করে?
- Topic, Partition, Offset, Broker — এক লাইনে কী?
- Producer আর Consumer কীভাবে কাজ করে?
- Consumer আর Consumer Group-এর পার্থক্য কী?
- Kafka কি মেসেজ consume হলে মুছে ফেলে? না হলে কখন মোছে?

### Partition ও Ordering (খুব common)
- Partition কেন দরকার? Scaling-এ এর ভূমিকা কী?
- Kafka কি পুরো topic-এ ordering দেয়? না দিলে কীভাবে ordering পাবেন?
- Partition key কীভাবে কাজ করে? একই key কি একই partition-এ যায়?
- একটা partition একসাথে কতজন consumer পড়তে পারে (একই group-এ)?
- Consumer সংখ্যা partition সংখ্যার বেশি হলে কী হয়?

### Delivery & Reliability
- At-most-once, at-least-once, exactly-once — পার্থক্য কী, কীভাবে পাবেন?
- `acks=0`, `acks=1`, `acks=all` — কোনটা কখন?
- ISR কী? `min.insync.replicas` কী কাজ করে?
- Idempotent producer কী সমস্যা সমাধান করে?
- Offset commit auto vs manual — কোনটা নিরাপদ, কেন?

### Consumer Group & Rebalancing
- Rebalance কখন হয়? এর খরচ কী?
- Consumer lag কী? কীভাবে monitor করবেন?
- Cooperative rebalance কী সুবিধা দেয়?
- `session.timeout.ms` আর `max.poll.interval.ms` কী নিয়ন্ত্রণ করে?

### Performance & Scaling
- Kafka এত দ্রুত কেন? (sequential write, zero-copy, batching, page cache)
- Throughput বাড়াতে কী কী tune করবেন?
- Partition সংখ্যা কীভাবে ঠিক করবেন?

### Practical / Scenario-based
- একটা consumer poison message-এ আটকে গেছে — কী করবেন?
- পুরনো ডেটা reprocess করতে হবে — কীভাবে?
- Broker crash করলে কী হয়? ডেটা হারায় কি?
- একই order-এর event উল্টো order-এ process হচ্ছে — সমাধান?

### Comparison
- Kafka vs RabbitMQ — কোনটা কখন?
- Kafka কি message queue নাকি অন্য কিছু?

### Advanced / Ecosystem
- ZooKeeper vs KRaft — পার্থক্য কী?
- Log compaction কী, কখন ব্যবহার করবেন?
- Kafka Connect, Kafka Streams, Schema Registry — কী কাজে লাগে?
- Exactly-once semantics Kafka কীভাবে দেয়?
- Tiered storage কী?

---


## ১৩. Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q30)

### Q1: Kafka কী এবং কোন সমস্যা সমাধান করে?
**উত্তর**: Kafka একটা **distributed, append-only log** — বিশাল পরিমাণ event real-time-এ ingest, store, ও multiple consumer-কে distribute করে, আর retention পর্যন্ত ডেটা রেখে দেয় বলে replay করা যায়। মূল সমস্যা যা সমাধান করে: **point-to-point integration spaghetti** (N×N সংযোগের বদলে একটা central log), high-throughput ingestion, আর একই ডেটা multiple team-এর মধ্যে শেয়ার করা।

### Q2: Topic, Partition, Offset, Broker — সম্পর্কটা কী?
**উত্তর**: **Topic** একটা logical feed (যেমন `orders`)। প্রতিটা topic এক বা একাধিক **Partition**-এ ভাগ — প্রতিটা partition একটা স্বাধীন ordered log। **Offset** হলো একটা partition-এর ভেতরে প্রতিটা record-এর ক্রমিক position (partition-scoped)। **Broker** হলো সার্ভার যেটা partition store/serve করে; কয়েকটা broker মিলে cluster।

```
Topic "orders"
 ├─ Partition 0:  [o0][o1][o2][o3] ...  (offset 0,1,2,3)
 ├─ Partition 1:  [o0][o1][o2] ...
 └─ Partition 2:  [o0][o1][o2][o3][o4] ...
```

### Q3: Kafka কীভাবে ordering guarantee দেয়?
**উত্তর**: **শুধু একটা partition-এর ভেতরে** ordering guaranteed, পুরো topic-এ নয়। একই entity-র সব event একই order-এ চাইলে সেই entity-র id কে **partition key** বানাতে হয় — তখন `hash(key) % partitions` সবসময় একই partition দেয়, আর সেই partition group-এর একজনই পড়ে।

```js
await producer.send({ topic: 'orders',
  messages: events.map(e => ({ key: orderId, value: JSON.stringify(e) })) });  // key → same partition
```

> সতর্কতা: retry-তে ordering ভাঙা এড়াতে `enable.idempotence=true` (এখন default)।

### Q4: Consumer আর Consumer Group-এর পার্থক্য?
**উত্তর**: একটা **Consumer** একটা প্রসেস যা partition পড়ে। একই `group.id`-র কয়েকটা consumer মিলে একটা **Consumer Group** — Kafka partition-গুলো এদের মধ্যে ভাগ করে দেয় (একটা partition একসাথে group-এর একজনই পড়ে)। **ভিন্ন group** একই topic সম্পূর্ণ independently পড়ে (নিজের offset, নিজের গতি)। এভাবেই load-sharing (এক group-এ) আর fan-out (একাধিক group) — দুটোই হয়।

### Q5: Kafka কি consume করলে মেসেজ মুছে ফেলে?
**উত্তর**: **না**। Consume করা মানে শুধু consumer নিজের offset এগিয়ে নেওয়া; ডেটা log-এ থেকে যায় **retention policy** (time/size) অনুযায়ী। তাই একই মেসেজ অনেক consumer পড়তে পারে ও পরে replay করা যায় — এটাই RabbitMQ থেকে মূল পার্থক্য (RabbitMQ ack-এর পর মুছে ফেলে)।

### Q6: `acks=0/1/all` — পার্থক্য ও কখন কোনটা?
**উত্তর**:
- **`acks=0`**: confirmation নেই — দ্রুততম, হারাতে পারে। (metrics, non-critical log)
- **`acks=1`**: leader লিখলেই confirm — leader fail-এ (replicate হওয়ার আগে) হারাতে পারে।
- **`acks=all`**: ISR-এর সব replica লিখলে confirm — সবচেয়ে নিরাপদ। (payment, order)

`acks=all` + `min.insync.replicas=2` + replication factor 3 = durability-র standard combo।

### Q7: ISR কী? `min.insync.replicas` কী করে?
**উত্তর**: **ISR (In-Sync Replicas)** = যেসব replica leader-এর সাথে যথেষ্ট up-to-date। **`min.insync.replicas=2`** মানে `acks=all`-এ অন্তত ২টা replica in-sync থাকতে হবে, নাহলে producer write **reject** হবে। এটা নিশ্চিত করে মেসেজ অন্তত ২ জায়গায় আছে — ১টা broker হারালেও ডেটা বাঁচে। Trade-off: in-sync replica ২-এর নিচে নামলে availability হারিয়ে write বন্ধ হয় (consistency আগে)।

### Q8: At-most / At-least / Exactly-once কীভাবে পাবেন?
**উত্তর**:
- **At-most-once**: process-এর **আগে** offset commit → crash-এ মেসেজ হারায়, duplicate নেই।
- **At-least-once**: process-এর **পরে** commit → duplicate হতে পারে, হারায় না। (সবচেয়ে common)
- **Exactly-once**: idempotent producer + **transaction** (consume+produce+offset commit atomic) + consumer-এ `read_committed`। downstream external effect থাকলে **idempotent consumer** (dedup key) বেশি নির্ভরযোগ্য।

### Q9: Idempotent producer কী সমস্যা সমাধান করে?
**উত্তর**: retry-র কারণে **duplicate write** ও **out-of-order write**। প্রতিটা producer-কে PID আর প্রতিটা record-এ partition-scoped sequence number দেওয়া হয়; broker duplicate/out-of-order ধরে বাদ দেয়। `enable.idempotence=true` (এখন default) — `acks=all` ও নিরাপদ retry নিশ্চিত করে। সীমা: এক producer session, এক partition — cross-partition/app-level dedup নয়।

### Q10: Rebalance কী? খরচ কী? কীভাবে কমাবেন?
**উত্তর**: Group-এ consumer যোগ/বিয়োগ হলে partition আবার ভাগ করা = **rebalance**। খরচ: চলাকালীন consumption সাময়িক থামে (stop-the-world)। কমানোর উপায়: **cooperative sticky assignor** (শুধু দরকারি partition সরায়), `session.timeout.ms`/`heartbeat` ঠিক রাখা, ভারী কাজ হলে `max.poll.interval.ms` বাড়ানো, static membership (`group.instance.id`) দিয়ে deploy-এ অপ্রয়োজনীয় rebalance এড়ানো।

### Q11: Consumer Lag কী? কেন গুরুত্বপূর্ণ?
**উত্তর**: **Lag = Log-End-Offset − Committed-Offset** — consumer produce-এর তুলনায় কতটা পিছিয়ে। Lag ক্রমাগত বাড়া মানে consumer ধরতে পারছে না; retention পার হয়ে গেলে ডেটা হারানোর ঝুঁকি। Monitor: `kafka-consumer-groups --describe`, Burrow, Prometheus। সমাধান: partition+consumer বাড়ানো, কাজ হালকা করা, batching।

### Q12: Kafka এত দ্রুত (high throughput) কেন?
**উত্তর**: চারটা মূল কারণ —
1. **Sequential disk write** (append-only log; random write নয়)
2. **Zero-copy** (`sendfile` — disk→network সরাসরি, app memory bypass)
3. **Batching + compression** (producer অনেক record একসাথে)
4. **OS page cache** (recent ডেটা প্রায় RAM-speed পড়া) + partition-based horizontal scaling।

### Q13: Broker crash করলে কী হয়? ডেটা হারায়?
**উত্তর**: Controller বুঝে ওই broker-এর leader partition-গুলোর জন্য **ISR থেকে নতুন leader** নির্বাচন করে; client metadata refresh করে নতুন leader-এ যায়। নতুন leader ISR-এ ছিল বলে **committed ডেটা হারায় না**। ব্যতিক্রম: `unclean.leader.election.enable=true` হলে out-of-sync replica leader হয়ে কিছু ডেটা হারাতে পারে — তাই production-এ `false` রাখা হয়।

### Q14: Partition সংখ্যা কীভাবে ঠিক করবেন? বাড়ানো-কমানো যায়?
**উত্তর**: Partition সংখ্যা = maximum consumer parallelism। target throughput ÷ প্রতি-partition throughput দিয়ে আন্দাজ করা হয়, সাথে ভবিষ্যৎ growth ধরে একটু বেশি রাখা হয়। **বাড়ানো যায়, কমানো যায় না**; আর বাড়ালে `hash(key) % partitions` বদলে যায় বলে existing key নতুন partition-এ যেতে পারে (ordering-এ প্রভাব)। খুব বেশি partition-ও খারাপ (বেশি open file, ধীর rebalance, বেশি metadata)।

### Q15: Log Compaction কী? কখন ব্যবহার করবেন?
**উত্তর**: `cleanup.policy=compact` — প্রতিটা **key-র শুধু সর্বশেষ value** রাখে, পুরনো গুলো মোছে। ব্যবহার: "current state" ডেটা (যেমন `userId → latest_profile`, DB changelog, `__consumer_offsets`)। key delete করতে **tombstone** (null value) পাঠানো হয়। ফলে log যত বড়ই হোক, প্রতিটা key-র current value সবসময় পাওয়া যায় — যেকোনো সময় state rebuild সম্ভব।

### Q16: ZooKeeper vs KRaft?
**উত্তর**: আগে Kafka cluster metadata ও controller election **ZooKeeper**-এ থাকতো (আলাদা সিস্টেম, বড় cluster-এ bottleneck)। **KRaft mode**-এ Kafka নিজেই Raft-based internal metadata log দিয়ে সব সামলায় — এক সিস্টেম (deploy সহজ), দ্রুত failover, লক্ষ লক্ষ partition scale। Kafka 3.3+ production-ready, 4.0-তে ZooKeeper বাদ।

### Q17: Poison message consumer-কে আটকে দিচ্ছে — সমাধান?
**উত্তর**: offset commit না করলে consumer একই মেসেজে আটকে থাকে, পুরো partition থামে। সমাধান: কয়েকবার retry-র পরও fail করলে মেসেজটা **Dead Letter Topic (DLT)**-এ পাঠিয়ে offset এগিয়ে দিন (Kafka-তে DLT নিজে বানাতে হয়)। Delay-সহ retry দরকার হলে `retry.5s`, `retry.30s` ধরনের আলাদা topic ব্যবহার করা হয়।

### Q18: পুরনো ডেটা reprocess/replay কীভাবে করবেন?
**উত্তর**: ডেটা retention-এর মধ্যে থাকলে consumer group-এর **offset reset** করুন:

```bash
kafka-consumer-groups.sh --group X --topic orders \
  --reset-offsets --to-datetime 2026-07-23T00:00:00.000 --execute
```
বা নতুন group-এ `fromBeginning:true` দিয়ে পুরো history পড়ুন (মূল pipeline অক্ষত রেখে)। RabbitMQ-তে এটা অসম্ভব — মেসেজ মুছে গেছে।

### Q19: Kafka vs RabbitMQ — কোনটা কখন?
**উত্তর**:
- **Kafka**: event streaming, log aggregation, high-throughput, multiple consumer একই ডেটা, replay দরকার (clickstream, IoT, CDC, analytics)।
- **RabbitMQ**: task queue, RPC, complex routing (priority/per-message TTL/DLQ built-in), একটা task একজন worker (payment command, job dispatch)।
এক লাইনে: **Kafka = "what happened" (event log), RabbitMQ = "do this" (task queue)।**

### Q20: `session.timeout.ms` vs `max.poll.interval.ms`?
**উত্তর**: **`session.timeout.ms`** — এই সময়ের মধ্যে heartbeat না এলে consumer মৃত ধরে rebalance। **`max.poll.interval.ms`** — দুই `poll()`-এর মধ্যে সর্বোচ্চ সময়; process করতে এর বেশি লাগলে consumer group থেকে বাদ পড়ে rebalance হয়। ভারী per-message কাজ থাকলে `max.poll.interval.ms` বাড়ান বা `max.poll.records` কমান।

### Q21: Exactly-once Kafka কীভাবে দেয়?
**উত্তর**: **Idempotent producer** (duplicate write বন্ধ) + **Transactions** — একই atomic transaction-এ produce ও input-offset commit, তাই "read-process-write" atomic। Consumer-এ `isolation.level=read_committed` দিলে শুধু committed transaction পড়ে। Kafka Streams এটা built-in দেয় (`processing.guarantee=exactly_once_v2`)। তবে external system (DB/API)-এ effect থাকলে idempotent consumer বেশি বাস্তবসম্মত।

### Q22: Producer-এ `max.in.flight.requests` আর ordering সম্পর্ক?
**উত্তর**: একাধিক request একসাথে in-flight থাকলে, একটা fail করে retry হওয়ার সময় পরেরটা আগে চলে যেতে পারে → ordering ভাঙে। **`enable.idempotence=true`** থাকলে Kafka in-flight ≤ 5 পর্যন্ত ordering ঠিক রাখে (sequence number দিয়ে)। idempotence ছাড়া strict order চাইলে `max.in.flight.requests.per.connection=1` করতে হতো (throughput কমতো)।

### Q23: Kafka Connect কী কাজে লাগে?
**উত্তর**: কোড না লিখে external system-এর সাথে Kafka যুক্ত করার framework। **Source connector** (DB→Kafka, যেমন Debezium CDC) আর **Sink connector** (Kafka→Elasticsearch/S3/DB)। scalable, fault-tolerant, offset management built-in — ETL/integration-এ boilerplate বাঁচায়।

### Q24: Kafka Streams vs ksqlDB vs Consumer API?
**উত্তর**:
- **Consumer API**: নিজে সব লজিক লেখেন (সর্বোচ্চ control)।
- **Kafka Streams**: stream processing library (map/filter/join/windowed aggregation, state store, exactly-once) — নিজের app-এ চলে, আলাদা cluster লাগে না।
- **ksqlDB**: Streams-এর উপর SQL layer — `CREATE STREAM ... SELECT ...`, দ্রুত prototyping ও simple pipeline।

### Q25: Retention শেষ হলে consumer না পড়া ডেটা কী হয়?
**উত্তর**: **মুছে যায়** — retention consumer-এর progress দেখে না, শুধু time/size দেখে। তাই consumer lag retention window পার করে ফেললে **ডেটা হারায়**। এজন্য lag monitoring জরুরি; দরকারে retention বাড়ান বা tiered storage ব্যবহার করুন।

### Q26: Schema Registry কেন দরকার?
**উত্তর**: Producer/consumer-এর মধ্যে schema contract enforce করতে। Avro/Protobuf schema registry-তে থাকে; শুধু compatible পরিবর্তন (backward/forward) allow হয়, তাই producer schema বদলালেও পুরনো consumer ভাঙে না। বোনাস: Avro/Protobuf binary JSON-এর চেয়ে ছোট ও দ্রুত।

### Q27: Kafka-তে message key-র ভূমিকা কী?
**উত্তর**: দুটো: (১) **Partitioning** — `hash(key) % partitions` দিয়ে কোন partition ঠিক করে, তাই একই key = একই partition = ordering। (২) **Compaction** — compacted topic-এ key-ই identity, প্রতি key-র সর্বশেষ value রাখা হয়। Key `null` হলে round-robin/sticky ভাবে partition-এ ছড়ায় (ordering guarantee নেই)।

### Q28: Consumer কি push নাকি pull? কেন গুরুত্বপূর্ণ?
**উত্তর**: Kafka consumer **pull** (নিজে `poll()` করে) — RabbitMQ push। ফায়দা: **natural backpressure** (consumer নিজের গতিতে নেয়, overload হয় না), efficient batching, আর replay সহজ (offset নিজে নিয়ন্ত্রণ করে)। খরচ: idle-এ সামান্য polling latency (`fetch.max.wait.ms` দিয়ে tune)।

### Q29: Tiered Storage কী সমস্যা সমাধান করে?
**উত্তর**: পুরনো segment স্থানীয় broker disk থেকে সস্তা **object storage (S3)**-এ সরিয়ে দেয়, recent ডেটা local-এ রাখে (KIP-405)। ফলে broker-এর local disk ছোট রেখেও মাসের পর মাসের ডেটা সস্তায় ধরে রাখা যায় — long-term replay/compliance/replay-heavy use case-এ scaling ও খরচ দুটোই ভালো হয়।

### Q30: Production Kafka-তে durability নিশ্চিত করতে কী কী config?
**উত্তর**: তিন layer একসাথে —
- **Producer**: `acks=all`, `enable.idempotence=true`, `retries` বেশি।
- **Broker/Topic**: `replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`।
- **Consumer**: `enable.auto.commit=false`, process-এর পরে manual commit (at-least-once) + idempotent consumer।
এর একটাও বাদ পড়লে (যেমন `acks=1`, বা auto-commit) মেসেজ হারানোর ফাঁক তৈরি হয়।

---

> 📘 RabbitMQ-র সমান্তরাল গাইড: [README.md](./README.md) — দুটো একসাথে পড়লে message broker আর event streaming-এর পুরো ছবি পরিষ্কার হয়ে যাবে।


