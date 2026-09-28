# Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q30)


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

> 📘 RabbitMQ-র সমান্তরাল গাইড: [01-RabbitMQ/](../01-RabbitMQ/) — দুটো একসাথে পড়লে message broker আর event streaming-এর পুরো ছবি পরিষ্কার হয়ে যাবে।

---

[⬅ 13-Interview-Questions-All-Levels.md](./13-Interview-Questions-All-Levels.md) | [🏠 Repo Home](../README.md)
