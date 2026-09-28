# Kafka কী এবং কেন — Real-life Examples


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

---

[⬅ RabbitMQ: 14-Interview-QA-Q23-Q35.md](../01-RabbitMQ/14-Interview-QA-Q23-Q35.md) | [02-Why-Kafka.md ➡](./02-Why-Kafka.md) | [🏠 Repo Home](../README.md)
