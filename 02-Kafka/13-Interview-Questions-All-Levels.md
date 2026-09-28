# ইন্টারভিউ প্রশ্ন — সব লেভেল


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

---

[⬅ 12-Real-World-Projects-Part2.md](./12-Real-World-Projects-Part2.md) | [14-Interview-QA-Q1-Q30.md ➡](./14-Interview-QA-Q1-Q30.md) | [🏠 Repo Home](../README.md)
