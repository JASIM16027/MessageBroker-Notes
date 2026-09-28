# কীভাবে কাজ করে — Flow


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

---

[⬅ 02-Why-Kafka.md](./02-Why-Kafka.md) | [04-Internals.md ➡](./04-Internals.md) | [🏠 Repo Home](../README.md)
