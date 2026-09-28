# Kafka বনাম RabbitMQ


এটা সবচেয়ে common ইন্টারভিউ প্রশ্ন। মূল পার্থক্যটা architecture-এ, আর সেটাই use case ঠিক করে দেয়। (RabbitMQ-র দিক থেকে এই তুলনার আরও বিস্তারিত [01-RabbitMQ/08-RabbitMQ-vs-Kafka.md](../01-RabbitMQ/08-RabbitMQ-vs-Kafka.md)-এ আছে।)

### মূল আর্কিটেকচারাল পার্থক্য

![RabbitMQ Queue Model vs Kafka Log Model](../images/05-rabbitmq-vs-kafka-model.png)

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

---

[⬅ 09-Retention-Compaction-Storage.md](./09-Retention-Compaction-Storage.md) | [11-Real-World-Projects-Part1.md ➡](./11-Real-World-Projects-Part1.md) | [🏠 Repo Home](../README.md)
