# Retention, Compaction ও Storage


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

---

[⬅ 08-High-Availability-Replication-ISR-KRaft.md](./08-High-Availability-Replication-ISR-KRaft.md) | [10-Kafka-vs-RabbitMQ.md ➡](./10-Kafka-vs-RabbitMQ.md) | [🏠 Repo Home](../README.md)
