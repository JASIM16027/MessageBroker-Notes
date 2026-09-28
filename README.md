# 📬 Message Broker Notes — RabbitMQ ও Apache Kafka

> বাংলায় গভীর, প্র্যাক্টিক্যাল নোট — **RabbitMQ** (message broker) ও **Apache Kafka** (event streaming platform) দুটোই একই কাঠামোয়: Beginner ধারণা → Internals → Reliability → Comparison → বাস্তব Project (কোডসহ) → Interview প্রশ্নোত্তর।

---

## 🎯 শেখার রোডম্যাপ — কোনটার পর কোনটা

সবচেয়ে কার্যকর ক্রম: **আগে বেসিক ধারণা → তারপর core mechanics → তারপর production-grade reliability → তারপর comparison → তারপর হাতে-কলমে project → সবশেষে interview revision।**

| ধাপ | Phase | RabbitMQ | Kafka |
|---|---|---|---|
| ১ | **Basics** (Beginner) | [01](./01-RabbitMQ/01-Introduction-and-Real-World-Examples.md) → [02](./01-RabbitMQ/02-Why-RabbitMQ.md) → [03](./01-RabbitMQ/03-How-It-Works-Flow.md) | [01](./02-Kafka/01-Introduction-and-Real-World-Examples.md) → [02](./02-Kafka/02-Why-Kafka.md) → [03](./02-Kafka/03-How-It-Works-Flow.md) |
| ২ | **Core Mechanics** | [04](./01-RabbitMQ/04-Internals.md) → [05](./01-RabbitMQ/05-Message-Ordering.md) | [04](./02-Kafka/04-Internals.md) → [05](./02-Kafka/05-Message-Ordering.md) → [06](./02-Kafka/06-Consumer-Group-Rebalancing.md) → [07](./02-Kafka/07-Delivery-Semantics.md) |
| ৩ | **Production & Reliability** | [06](./01-RabbitMQ/06-High-Availability-Quorum-Raft.md) → [07](./01-RabbitMQ/07-Mirrored-Queue-Deprecated.md) | [08](./02-Kafka/08-High-Availability-Replication-ISR-KRaft.md) → [09](./02-Kafka/09-Retention-Compaction-Storage.md) |
| ৪ | **Decision / Comparison** | [08 — RabbitMQ vs Kafka](./01-RabbitMQ/08-RabbitMQ-vs-Kafka.md) | [10 — Kafka vs RabbitMQ](./02-Kafka/10-Kafka-vs-RabbitMQ.md) |
| ৫ | **হাতে-কলমে Practice** | [09](./01-RabbitMQ/09-Real-World-Projects-Part1.md) + [10](./01-RabbitMQ/10-Real-World-Projects-Part2.md) — ১৮টি case | [11](./02-Kafka/11-Real-World-Projects-Part1.md) + [12](./02-Kafka/12-Real-World-Projects-Part2.md) — ১৬টি case |
| ৬ | **Interview Revision** | [11](./01-RabbitMQ/11-Interview-Questions-All-Levels.md) → [12](./01-RabbitMQ/12-Interview-QA-Q1-Q8.md) → [13](./01-RabbitMQ/13-Interview-QA-Q9-Q22.md) → [14](./01-RabbitMQ/14-Interview-QA-Q23-Q35.md) | [13](./02-Kafka/13-Interview-Questions-All-Levels.md) → [14](./02-Kafka/14-Interview-QA-Q1-Q30.md) |

> **টিপস**: প্রথমবার পড়ার সময় ধাপ ১–৪ ভালোভাবে বুঝে তারপর ধাপ ৫-এ (Real-World Projects) নিজে কোড লিখে দেখুন। ধাপ ৬ (Interview) সবশেষে revision হিসেবে রাখুন — তখন আগের সব ধারণা একসাথে ঝালাই হয়ে যাবে।

---

## 📂 Repository Structure

```
01-RabbitMQ/
├── 01-Introduction-and-Real-World-Examples.md   → কী, কেন, real-life example
├── 02-Why-RabbitMQ.md                           → যে সমস্যা সমাধান করে
├── 03-How-It-Works-Flow.md                      → Producer → Exchange → Queue → Consumer → Ack
├── 04-Internals.md                              → Connection/Channel, Exchange Types, Durability, Prefetch, DLQ
├── 05-Message-Ordering.md                       → কেন ordering ভাঙে, কীভাবে ঠিক রাখা যায়
├── 06-High-Availability-Quorum-Raft.md          → Cluster, Quorum Queue, Raft consensus
├── 07-Mirrored-Queue-Deprecated.md              → পুরনো HA পদ্ধতি, কেন বাদ পড়লো
├── 08-RabbitMQ-vs-Kafka.md                      → আর্কিটেকচারাল পার্থক্য, কখন কোনটা
├── 09-Real-World-Projects-Part1.md              → সমস্যা ১–৯ (কোডসহ)
├── 10-Real-World-Projects-Part2.md              → সমস্যা ১০–১৮ (কোডসহ)
├── 11-Interview-Questions-All-Levels.md         → প্রশ্ন তালিকা (বিভাগভিত্তিক)
├── 12-Interview-QA-Q1-Q8.md                     → বিস্তারিত উত্তর
├── 13-Interview-QA-Q9-Q22.md                    → বিস্তারিত উত্তর
└── 14-Interview-QA-Q23-Q35.md                   → বিস্তারিত উত্তর

02-Kafka/
├── 01-Introduction-and-Real-World-Examples.md
├── 02-Why-Kafka.md
├── 03-How-It-Works-Flow.md
├── 04-Internals.md                              → Topic, Partition, Offset, Broker
├── 05-Message-Ordering.md
├── 06-Consumer-Group-Rebalancing.md
├── 07-Delivery-Semantics.md                     → at-most/at-least/exactly-once
├── 08-High-Availability-Replication-ISR-KRaft.md
├── 09-Retention-Compaction-Storage.md
├── 10-Kafka-vs-RabbitMQ.md
├── 11-Real-World-Projects-Part1.md              → সমস্যা ১–৯ (কোডসহ)
├── 12-Real-World-Projects-Part2.md              → সমস্যা ১০–১৬ (কোডসহ)
├── 13-Interview-Questions-All-Levels.md
└── 14-Interview-QA-Q1-Q30.md

images/           → diagram PNG + Mermaid source (images/src/)
```

---

## 🐰 RabbitMQ — সব সেকশন

| # | Note |
|---|---|
| 1 | [RabbitMQ কী এবং কেন — Real-life Examples](./01-RabbitMQ/01-Introduction-and-Real-World-Examples.md) |
| 2 | [যে সমস্যা RabbitMQ সমাধান করে](./01-RabbitMQ/02-Why-RabbitMQ.md) |
| 3 | [কীভাবে কাজ করে — Flow](./01-RabbitMQ/03-How-It-Works-Flow.md) |
| 4 | [ভেতরের মেকানিজম — Internals](./01-RabbitMQ/04-Internals.md) |
| 5 | [Message Ordering — বিস্তারিত](./01-RabbitMQ/05-Message-Ordering.md) |
| 6 | [High Availability — Cluster, Quorum Queue ও Raft](./01-RabbitMQ/06-High-Availability-Quorum-Raft.md) |
| 7 | [Mirrored Queue — পুরনো ও Deprecated পদ্ধতি](./01-RabbitMQ/07-Mirrored-Queue-Deprecated.md) |
| 8 | [RabbitMQ vs Kafka](./01-RabbitMQ/08-RabbitMQ-vs-Kafka.md) |
| 9 | [Real-World Projects, সমস্যা ১–৯ (কোডসহ)](./01-RabbitMQ/09-Real-World-Projects-Part1.md) |
| 10 | [Real-World Projects, সমস্যা ১০–১৮ (কোডসহ)](./01-RabbitMQ/10-Real-World-Projects-Part2.md) |
| 11 | [ইন্টারভিউ প্রশ্ন — সব লেভেল](./01-RabbitMQ/11-Interview-Questions-All-Levels.md) |
| 12 | [Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q8)](./01-RabbitMQ/12-Interview-QA-Q1-Q8.md) |
| 13 | [আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q9–Q22)](./01-RabbitMQ/13-Interview-QA-Q9-Q22.md) |
| 14 | [আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q23–Q35)](./01-RabbitMQ/14-Interview-QA-Q23-Q35.md) |

## 📗 Apache Kafka — সব সেকশন

| # | Note |
|---|---|
| 1 | [Kafka কী এবং কেন — Real-life Examples](./02-Kafka/01-Introduction-and-Real-World-Examples.md) |
| 2 | [যে সমস্যা Kafka সমাধান করে](./02-Kafka/02-Why-Kafka.md) |
| 3 | [কীভাবে কাজ করে — Flow](./02-Kafka/03-How-It-Works-Flow.md) |
| 4 | [ভেতরের মেকানিজম — Internals (Topic/Partition/Offset/Broker)](./02-Kafka/04-Internals.md) |
| 5 | [Message Ordering — বিস্তারিত](./02-Kafka/05-Message-Ordering.md) |
| 6 | [Consumer Group ও Rebalancing](./02-Kafka/06-Consumer-Group-Rebalancing.md) |
| 7 | [Delivery Semantics — Guarantee](./02-Kafka/07-Delivery-Semantics.md) |
| 8 | [High Availability — Replication, ISR, KRaft](./02-Kafka/08-High-Availability-Replication-ISR-KRaft.md) |
| 9 | [Retention, Compaction ও Storage](./02-Kafka/09-Retention-Compaction-Storage.md) |
| 10 | [Kafka বনাম RabbitMQ](./02-Kafka/10-Kafka-vs-RabbitMQ.md) |
| 11 | [Real-World Projects, সমস্যা ১–৯ (কোডসহ)](./02-Kafka/11-Real-World-Projects-Part1.md) |
| 12 | [Real-World Projects, সমস্যা ১০–১৬ (কোডসহ)](./02-Kafka/12-Real-World-Projects-Part2.md) |
| 13 | [ইন্টারভিউ প্রশ্ন — সব লেভেল](./02-Kafka/13-Interview-Questions-All-Levels.md) |
| 14 | [Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q30)](./02-Kafka/14-Interview-QA-Q1-Q30.md) |

---

## 🚀 কোথা থেকে শুরু করবেন?

| আপনার লক্ষ্য | এখান থেকে শুরু করুন |
|---|---|
| 🌱 একদম নতুন, message broker কী জানি না | [RabbitMQ পরিচিতি](./01-RabbitMQ/01-Introduction-and-Real-World-Examples.md) থেকে ক্রমানুসারে |
| 🛠 Hands-on শিখতে চাই | সরাসরি [Real-World Projects](./01-RabbitMQ/09-Real-World-Projects-Part1.md)-এ যান, কোড রান করে দেখুন |
| ⚖️ RabbitMQ vs Kafka কনফিউশন দূর করতে চাই | [RabbitMQ vs Kafka](./01-RabbitMQ/08-RabbitMQ-vs-Kafka.md) |
| 💼 Job interview আসছে | [RabbitMQ Interview Q&A](./01-RabbitMQ/11-Interview-Questions-All-Levels.md) → [Kafka Interview Q&A](./02-Kafka/13-Interview-Questions-All-Levels.md) |

---

## 🧭 কীভাবে পড়বেন

1. প্রতিটা নোটের শুরুতে core concept, শেষে **prev/next navigation link** আছে — ক্রমানুসারে পড়তে থাকুন।
2. প্রতিটা Real-World Project সমস্যায় একটা কোড উদাহরণ + "🔍 আরও গভীরে" সেকশন আছে যেখানে প্রতিটা লাইন কেন লেখা হলো তা ব্যাখ্যা করা — শুধু কোড কপি না করে এই অংশটা মন দিয়ে পড়ুন।
3. RabbitMQ ও Kafka সমান্তরালভাবে পড়লে পার্থক্যগুলো বেশি স্পষ্ট হবে (প্রতিটা ধাপে দুটো গাইডেরই লিংক পাশাপাশি দেওয়া আছে উপরের রোডম্যাপ টেবিলে)।

### ✍️ নতুন note যোগ করার নিয়ম

- নতুন section → সংশ্লিষ্ট `01-RabbitMQ/` বা `02-Kafka/` ফোল্ডারে পরবর্তী নাম্বার দিয়ে ফাইল বানান।
- এই README-র টেবিলে লিংক যোগ করুন এবং আগের শেষ ফাইলের নেভিগেশন footer আপডেট করুন।
- নতুন diagram → `images/src/`-এ `.mmd` (Mermaid) ফাইল লিখে render করুন:
  `npx -p @mermaid-js/mermaid-cli mmdc -i images/src/xx.mmd -o images/xx.png -b white -s 2`

---

## 🤝 Contribute / ভুল পেলে

- কোনো তথ্য ভুল বা পুরনো মনে হলে **Issue** খুলুন, অথবা সরাসরি **Pull Request** দিন।
- RabbitMQ/Kafka-র config, default value প্রায়ই ভার্সনে বদলায়, তাই গুরুত্বপূর্ণ সিদ্ধান্তের আগে official docs মিলিয়ে নিন।

<div align="center">

⭐ এই note কাজে লাগলে repo-তে একটা **Star** দিন, যাতে অন্যরাও খুঁজে পায়।

</div>
