# RabbitMQ — সম্পূর্ণ গাইড

> RabbitMQ হচ্ছে একটা **message broker** — মানে দুইটা সিস্টেমের মধ্যে মেসেজ পাঠানো-আনানোর কাজ করে, যাতে তারা একসাথে (synchronously) কাজ না করেও একে অপরের সাথে যোগাযোগ করতে পারে।

## সূচিপত্র (Table of Contents)

1. [RabbitMQ কী এবং কেন — Real-life Examples](#১-rabbitmq-কী-এবং-কেন--real-life-examples)
2. [যে সমস্যা সমাধান করে](#২-যে-সমস্যা-সমাধান-করে)
3. [কীভাবে কাজ করে — Flow](#৩-কীভাবে-কাজ-করে--flow)
4. [ভেতরের মেকানিজম — Internals](#৪-ভেতরের-মেকানিজম--internals)
5. [Message Ordering — বিস্তারিত](#৫-message-ordering--বিস্তারিত)
6. [High Availability — Cluster, Quorum Queue ও Raft](#৬-high-availability--cluster-quorum-queue-ও-raft)
7. [Mirrored Queue — পুরনো ও Deprecated পদ্ধতি](#৭-mirrored-queue--পুরনো-ও-deprecated-পদ্ধতি)
8. [RabbitMQ vs Kafka](#৮-rabbitmq-vs-kafka)
9. [ইন্টারভিউ প্রশ্ন — সব লেভেল](#৯-ইন্টারভিউ-প্রশ্ন--সব-লেভেল)
10. [Advanced ইন্টারভিউ প্রশ্ন-উত্তর](#১০-advanced-ইন্টারভিউ-প্রশ্ন-উত্তর)
11. [আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q9–Q22)](#১১-আরও-গভীর-ইন্টারভিউ-প্রশ্ন-উত্তর-q9q22)
12. [Real-World Project — সমস্যা ও সমাধান (কোডসহ)](#১২-real-world-project--সমস্যা-ও-সমাধান-কোডসহ)

---

## ১. RabbitMQ কী এবং কেন — Real-life Examples

RabbitMQ হচ্ছে একটা **message broker** — মানে দুইটা সিস্টেমের মধ্যে মেসেজ পাঠানো-আনানোর কাজ করে, যাতে তারা একসাথে (synchronously) কাজ না করেও একে অপরের সাথে যোগাযোগ করতে পারে। নিচে কিছু real life example দিলাম:

### ১. Food Delivery App (যেমন Foodpanda/Pathao)
- আপনি অর্ডার দিলেন → Order Service সাথে সাথে RabbitMQ-তে একটা মেসেজ পাঠায়
- সেই মেসেজ কয়েকটা আলাদা service consume করে:
  - Restaurant notification service (রেস্টুরেন্টকে জানায়)
  - Payment service (টাকা কাটে)
  - SMS/Push notification service (আপনাকে জানায় "অর্ডার confirmed")
  - Analytics service (ডেটা সংরক্ষণ করে)

**ফায়দা**: এক service slow হলে বা ডাউন থাকলেও, আপনার অর্ডার confirmation আটকায় না। মেসেজ queue-তে জমা থাকে, service ঠিক হলে পরে process হয়।

### ২. E-commerce Order Processing (Daraz/Amazon টাইপ)
- Order placed হওয়ার পর inventory update, invoice generation, shipping label তৈরি — এই সব কাজ আলাদা আলাদা queue-তে ভাগ করে দেওয়া হয়
- প্রতিটা কাজ independently, নিজের গতিতে process হয়

### ৩. Video/Image Processing (YouTube টাইপ)
- ইউজার ভিডিও আপলোড করলে সাথে সাথে সেটা প্রসেস (compress, thumbnail তৈরি, বিভিন্ন resolution-এ convert) করতে সময় লাগে
- আপলোড হওয়ার সাথে সাথে RabbitMQ-তে একটা "job" পাঠানো হয়
- Background worker সেই job নিয়ে ধীরে ধীরে প্রসেস করে, ইউজারকে অপেক্ষা করতে হয় না

### ৪. Email/SMS Notification System
- হাজার হাজার ইউজারকে একসাথে ইমেইল পাঠাতে হবে (যেমন newsletter)
- মূল অ্যাপ্লিকেশন প্রতিটা ইমেইলের জন্য RabbitMQ-তে একটা task পাঠায়
- আলাদা "worker" process গুলো ধীরে ধীরে queue থেকে নিয়ে ইমেইল পাঠায়, মূল অ্যাপ blocked হয় না

### ৫. Ride-Sharing App (Uber/Pathao)
- ড্রাইভার আর রাইডার ম্যাচ করার সময় বিভিন্ন সার্ভিস (location tracking, fare calculation, driver notification) একসাথে কাজ করে
- RabbitMQ দিয়ে এই সার্ভিসগুলো একে অপরকে event পাঠায় (যেমন "ride_requested", "driver_assigned")

### মূল ধারণাটা কী?
সব উদাহরণেই একটা common pattern: **একটা কাজ হয়ে গেলে, তার পরের ধাপগুলো সাথে সাথে করার দরকার নেই, বরং queue-তে রেখে দেওয়া যায় এবং ব্যাকগ্রাউন্ডে প্রসেস করা যায়।** এতে:
- মূল অ্যাপ fast থাকে (ইউজার wait করে না)
- Traffic spike হ্যান্ডেল করা যায় সহজে
- কোনো একটা service ডাউন থাকলেও পুরো সিস্টেম ভেঙে পড়ে না

---

## ২. যে সমস্যা সমাধান করে

RabbitMQ কীভাবে কাজ করে এবং কী সমস্যা সমাধান করে, ধাপে ধাপে বুঝিয়ে দিচ্ছি।

ধরুন আপনার একটা অ্যাপ্লিকেশন আছে, যেখানে User A একটা action করলে (যেমন সাইন আপ) তারপর ৫টা কাজ হওয়া দরকার:
1. Database-এ save করা
2. Welcome email পাঠানো
3. SMS পাঠানো
4. Analytics-এ log করা
5. Third-party CRM-এ sync করা

**RabbitMQ ছাড়া (traditional way)** এই সব কাজ যদি একটার পর একটা সরাসরি (synchronously) করেন, তাহলে:
- Email service slow হলে বা ডাউন থাকলে পুরো request আটকে থাকবে
- User-কে "please wait" বলে বসিয়ে রাখতে হবে
- একটা service fail করলে পুরো chain ভেঙে যেতে পারে
- সব service কে একসাথে, tightly coupled ভাবে চালাতে হবে

**RabbitMQ দিয়ে** — Sign up হওয়ার সাথে সাথে শুধু একটা মেসেজ পাঠিয়ে দেন queue-তে, আর সাথে সাথে user-কে "success" response দিয়ে দেন। বাকি কাজগুলো ব্যাকগ্রাউন্ডে, আলাদা আলাদা service যার যার সময়ে সম্পন্ন করে।

---

## ৩. কীভাবে কাজ করে — Flow

এই ছবিতে পুরো flow টা দেখা যাচ্ছে। এখন ধাপে ধাপে ব্যাখ্যা করি:

### ১. Producer
আপনার অ্যাপ্লিকেশন (যেমন একটা API server) একটা মেসেজ তৈরি করে RabbitMQ-তে পাঠায়। এই কাজটা সাথে সাথে হয়ে যায় — producer কোনো response wait করে না।

### ২. Exchange
মেসেজটা সরাসরি Queue-তে যায় না, প্রথমে **Exchange**-এ যায়। Exchange-এর কাজ হলো ঠিক করা মেসেজটা কোন Queue-তে পাঠানো হবে — এটা routing key বা binding rule অনুযায়ী ঠিক হয় (আগের মেসেজে যে Direct/Fanout/Topic exchange বলেছিলাম, সেটার কাজ এখানেই)।

### ৩. Queue
মেসেজটা Queue-তে জমা থাকে (buffer এর মতো) — যতক্ষণ না কোনো Consumer সেটা নিয়ে যায়। এখানেই মূল ফায়দাটা — Consumer এই মুহূর্তে ব্যস্ত থাকলে বা ডাউন থাকলেও মেসেজ হারায় না, Queue-তে অপেক্ষা করে।

### ৪. Consumer
একটা worker process Queue থেকে মেসেজ নিয়ে প্রসেস করে (যেমন email পাঠানো, database update করা)।

### ৫. Acknowledgement (Ack)
এটাই সবচেয়ে গুরুত্বপূর্ণ part। Consumer কাজ শেষ করার পর RabbitMQ-কে "ack" (acknowledgement) পাঠায়। তখনই RabbitMQ মেসেজটা Queue থেকে permanently ডিলিট করে।

**যদি Consumer কাজ করার আগেই crash করে** — ack পাঠানো হয় না, তাই RabbitMQ মেসেজটা মুছে ফেলে না, বরং সেটা আবার Queue-তে ফিরিয়ে দেয় (বা অন্য কোনো Consumer-কে দেয়)। এভাবে **guaranteed delivery** নিশ্চিত হয় — মেসেজ হারানোর কোনো সুযোগ নেই।

এই জন্যই Food delivery বা E-commerce এর example এ বলেছিলাম — একটা service crash করলেও ডেটা হারায় না, শুধু delay হয়।

---

## ৪. ভেতরের মেকানিজম — Internals

আরও গভীরে যাই, একটা একটা করে RabbitMQ-এর ভেতরের মেকানিজম বুঝিয়ে দিচ্ছি।

### Connection আর Channel — পার্থক্য কী

- **Connection**: আপনার অ্যাপ্লিকেশন আর RabbitMQ সার্ভারের মধ্যে একটা TCP connection। এটা তৈরি করা costly (resource-heavy)।
- **Channel**: একটা Connection-এর ভেতরে অনেকগুলো "virtual" চ্যানেল তৈরি করা যায়। প্রতিটা কাজ (publish, consume) একটা Channel দিয়ে হয়।

**কেন দরকার?** প্রতিটা মেসেজ পাঠানোর জন্য নতুন TCP connection বানালে সিস্টেম স্লো হয়ে যাবে। তাই একটা Connection খুলে তার ভেতরে অনেক Channel দিয়ে multiple কাজ parallel-এ চালানো হয় — হালকা এবং দ্রুত।

### Exchange Types — বিস্তারিত

চার ধরনের Exchange হয় — **Direct, Fanout, Topic, Headers** — প্রতিটার নিজস্ব routing behavior ও use case আছে:

| Type | কাজ | Use case |
|---|---|---|
| **Direct** | routing key হুবহু ম্যাচ করে যে queue-তে পাঠায় | নির্দিষ্ট queue-তে মেসেজ পাঠানো |
| **Fanout** | সব bound queue-তে broadcast করে | সব consumer-কে একই মেসেজ পাঠানো |
| **Topic** | pattern (wildcard) দিয়ে routing key ম্যাচ করে | category-ভিত্তিক routing |
| **Headers** | message header দিয়ে routing করে | header-based complex routing |

### Reliability / Durability — মেসেজ কীভাবে হারায় না

তিনটা জিনিস একসাথে configure করতে হয়, নাহলে RabbitMQ সার্ভার রিস্টার্ট হলে বা crash করলে মেসেজ হারিয়ে যাবে:

1. **Durable Queue** — Queue declare করার সময় `durable: true` সেট করতে হয়, নাহলে সার্ভার রিস্টার্ট হলে Queue-টাই উধাও হয়ে যাবে।
2. **Persistent Message** — মেসেজ পাঠানোর সময় `delivery_mode: 2` সেট করলে মেসেজ disk-এ লেখা হয় (শুধু RAM-এ না)।
3. **Publisher Confirm** — Producer কে জানানো হয় যে RabbitMQ মেসেজটা সত্যিই receive করেছে, নাহলে producer ভাববে মেসেজ গেছে কিন্তু আসলে যায়নি।

### Ack — Manual vs Automatic

- **Auto ack**: মেসেজ Consumer-কে দেওয়া মাত্রই RabbitMQ ধরে নেয় কাজ শেষ, মুছে ফেলে। **সমস্যা**: Consumer যদি মেসেজ পাওয়ার পরই crash করে, মেসেজ হারিয়ে যায়।
- **Manual ack**: Consumer কাজ শেষ করার পর explicitly `ack()` কল করে। এটাই production-এ recommended, কারণ কাজ পুরোপুরি সম্পন্ন না হওয়া পর্যন্ত RabbitMQ মেসেজ ধরে রাখে।

### Prefetch Count — কেন গুরুত্বপূর্ণ

ধরুন একটা Queue-তে ১০০০০ মেসেজ আছে, আর একটা Consumer আছে। Prefetch সেট না করলে RabbitMQ সব মেসেজ একসাথে consumer-কে পাঠিয়ে দেবে — consumer এর memory overflow হবে। `prefetch_count: 10` সেট করলে consumer একসাথে সর্বোচ্চ ১০টা মেসেজ নিয়ে কাজ করবে, একটার ack দিলে পরের একটা আসবে। এভাবে **load evenly distribute** হয় multiple consumer এর মধ্যে।

### Dead Letter Queue (DLQ)

একটা মেসেজ যদি বারবার প্রসেস করতে গিয়ে fail করে (exception throw করে), তাহলে সেটা infinite loop-এ আটকে যেতে পারে। DLQ হলো একটা আলাদা "reject/expired মেসেজের জন্য" Queue। যখন কোনো মেসেজ:
- Consumer explicitly reject করে (`nack` with requeue=false)
- একটা নির্দিষ্ট সময় (TTL) পার হয়ে যায়
- Queue এর সর্বোচ্চ length ছাড়িয়ে যায়

তখন সেটা মূল Queue থেকে সরে গিয়ে DLQ-তে চলে যায়, যাতে পরে manually inspect করা যায় কেন fail হয়েছিল — মূল Queue আটকে না থেকে।

---

## ৫. Message Ordering — বিস্তারিত

আরও বিস্তারিতভাবে বুঝিয়ে দিচ্ছি এই order guarantee ব্যাপারটা।

### ৫.১ কেন Multiple Consumer থাকলে Order ভেঙে যায়

ধরুন একটা Queue-তে ৩টা মেসেজ আছে account "A" এর জন্য: `deposit_100`, `withdraw_50`, `deposit_20` — এই order-এই আসছে।

যদি ২টা Consumer (C1, C2) থাকে RabbitMQ **round-robin** ভাবে মেসেজ ভাগ করে দেয়:
- `deposit_100` → C1
- `withdraw_50` → C2
- `deposit_20` → C1

এখন C2 যদি slow হয় (নেটওয়ার্ক লেটেন্সি, বা heavy computation), তাহলে C1 তার দুটো মেসেজ (`deposit_100`, `deposit_20`) আগেই শেষ করে ফেলতে পারে, কিন্তু `withdraw_50` তখনও process হয়নি। ফলে balance calculation-এ ভুল হয়ে যাবে — কারণ **processing order আর queue order এক থাকছে না**।

### ৫.২ সমাধান ১: Routing Key দিয়ে Same Account = Same Queue

এখানে মূল কৌশল হলো — একই account-এর সব মেসেজ **যেন সবসময় একই Queue-তে যায়**, এবং সেই Queue-এর সাথে **একটাই Consumer** bind করা থাকে।

কীভাবে করা হয়:
- Producer মেসেজ পাঠানোর সময় routing key হিসেবে `account_id` ব্যবহার করে (যেমন routing key = `"account_12345"`)
- **Consistent Hashing Exchange** (এটা RabbitMQ-এর একটা প্লাগইন) সেই routing key-কে hash করে ঠিক করে কোন Queue-তে যাবে
- একই routing key সবসময় একই hash bucket-এ পড়বে, তাই একই account-এর সব মেসেজ সবসময় একই Queue-তে যাবে
- সেই Queue-এর সাথে fix করা একটাই Consumer কাজ করবে, তাই ordering বজায় থাকবে

**এভাবে scaling ও বজায় থাকে**: হাজার হাজার account থাকলেও, ১০০টা Queue বানিয়ে সব account hash করে ভাগ করে দিলে, প্রতিটা Queue-এর ভেতরে order ঠিক থাকবে, আবার overall system parallel-এও কাজ করবে (১০০টা Queue = ১০০টা consumer parallel-এ কাজ করছে, শুধু একই account-এর ভেতরে order মেনে চলছে)।

### ৫.৩ সমাধান ২: Single Active Consumer (RabbitMQ built-in feature)

RabbitMQ-তে `x-single-active-consumer` নামে একটা ফিচার আছে — একটা Queue-তে অনেকগুলো Consumer subscribe করে রাখতে পারে, কিন্তু RabbitMQ যেকোনো সময় শুধু **একটাকে active** রাখে। বাকিগুলো standby-তে থাকে।

**ফায়দা**: Order বজায় থাকে (কারণ একসাথে একজনই process করছে), আবার active consumer crash করলে RabbitMQ automatically আরেকজন standby consumer-কে active করে দেয় — তাই High Availability-ও পাওয়া যায়।

### ৫.৪ Real World: ব্যাংক Transaction System — বিস্তারিত ফ্লো

ধরুন account নাম্বার `ACC-9981`:

1. User একটা deposit করলো → Order Service মেসেজ পাঠায় routing key `ACC-9981` দিয়ে
2. একই সেকেন্ডে সেই account-এই একটা withdrawal হলো → routing key `ACC-9981` দিয়ে আরেকটা মেসেজ
3. Consistent Hashing Exchange দুটো মেসেজকেই একই Queue (ধরুন `queue_shard_7`) এ পাঠায়, কারণ routing key একই
4. `queue_shard_7`-এর সাথে বাঁধা consumer মেসেজ দুটো ঠিক order-এই (deposit আগে, withdraw পরে) process করে
5. অন্য account (`ACC-4432`)-এর মেসেজ হয়তো `queue_shard_2`-এ যাচ্ছে, সম্পূর্ণ আলাদা consumer-এ, parallel-এ প্রসেস হচ্ছে — কোনো ব্লকিং হচ্ছে না

**মূল কথা**: Ordering per-entity (per account, per user, per order) দরকার হয়, পুরো সিস্টেমে global ordering দরকার হয় না। তাই "একই entity-র মেসেজ একই queue-তে" — এই principle মেনে চললেই বাস্তবে বেশিরভাগ use case কভার হয়ে যায়।

### ৫.৫ আরও সহজভাবে — একদম মৌলিক থেকে

আরও সহজভাবে, ধাপে ধাপে, একদম মৌলিক থেকে বুঝিয়ে দিচ্ছি।

#### প্রথমে বুঝি সমস্যাটা কোথায়

কল্পনা করুন আপনার একটা মাত্র Queue আছে আর একটা মাত্র Consumer। তাহলে সমস্যাই নেই — মেসেজ যে order এ আসে, সেই order এই যায়, সেই order এই process হয়। এক লাইনে মানুষ যেভাবে টিকিট কাউন্টারে দাঁড়ায়, ঠিক সেভাবে।

**সমস্যা তখনই আসে যখন আপনি স্পিড বাড়ানোর জন্য একাধিক Consumer লাগান।** ছবিতে পুরো ব্যাপারটা দুই ভাগে দেখানো হলো — সমস্যা আর সমাধান। এবার লিখেও একদম সহজ ভাষায় বুঝি।

#### সমস্যাটা আসলে কী

একটা Queue-তে মেসেজ সবসময় ঠিক order-এই ঢোকে। কিন্তু যখন সেই Queue থেকে দুইজন Consumer (C1, C2) একসাথে মেসেজ তুলে নেয়, তখন কে কোনটা কতক্ষণে শেষ করবে সেটা RabbitMQ নিয়ন্ত্রণ করতে পারে না। ফলে:
- C1 হয়তো দ্রুত কাজ শেষ করলো
- C2 slow (হয়তো তার মেসেজ প্রসেস করতে বেশি সময় লাগছে)
- ফলাফল: যে মেসেজটা পরে queue-তে ঢুকেছিল, সেটা আগেই process হয়ে গেলো

এটা ব্যাংকের ক্ষেত্রে ভয়ংকর — deposit-এর আগে withdraw process হয়ে গেলে account balance ভুল হিসাব হতে পারে।

#### সমাধানের মূল আইডিয়া — এক লাইনে

**"একই জিনিসের (যেমন একই account) সব মেসেজ যেন সবসময় একই লাইনে (Queue), একই লোকের (Consumer) কাছে যায়।"**

এটা বাস্তব জীবনের উদাহরণ দিয়ে বুঝি — ধরুন একটা ব্যাংকে ৫টা কাউন্টার আছে। আপনি চান একই কাস্টমারের সব কাজ (deposit, withdraw) সবসময় একই কাউন্টারে হোক, যাতে সেই কাউন্টারের লোকটা ক্রমানুসারে কাজগুলো করে। কিন্তু ভিন্ন ভিন্ন কাস্টমার ভিন্ন ভিন্ন কাউন্টারে যেতে পারে — তাতে সমস্যা নেই, কারণ তাদের কাজের মধ্যে কোনো order dependency নেই।

#### কীভাবে এটা টেকনিক্যালি হয়

1. প্রতিটা মেসেজের সাথে একটা "identifier" জুড়ে দেওয়া হয় — account ID, user ID, বা order ID
2. **Hash Exchange** এই identifier-টা নিয়ে একটা গাণিতিক হিসাব (hashing) করে ঠিক করে এটা কোন Queue-তে যাবে
3. **গুরুত্বপূর্ণ ব্যাপার**: একই identifier সবসময় একই হিসাব দেবে, তাই একই account-এর মেসেজ *সবসময়* একই Queue-তে যাবে — এটা random নয়, deterministic (নির্ধারিত)
4. প্রতিটা Queue-এর সাথে একটামাত্র Consumer বাঁধা থাকে, তাই সেই Queue-এর ভেতরে order একদম ঠিক থাকে

#### কেন এটা speed-ও কমায় না

আপনার হয়তো মনে হচ্ছে — "তাহলে তো সবকিছু একজন Consumer-ই করছে, স্লো হয়ে যাবে না?"

না, কারণ সব account একই Queue-তে যাচ্ছে না। হাজার হাজার account থাকলে সেগুলো ভাগ হয়ে হয়তো ১০০টা আলাদা Queue-তে ছড়িয়ে যাচ্ছে, প্রতিটার একটা করে Consumer। তাই:
- **প্রতিটা account-এর ভেতরে**: order ঠিক থাকছে (একই queue, একই consumer)
- **overall system-এ**: ১০০টা Queue parallel-এ কাজ করছে, তাই throughput ও ভালো থাকছে

এটাই মূল কৌশল — পুরো সিস্টেমে order লাগে না, লাগে শুধু **related জিনিসগুলোর মধ্যে** order। এই ছোট্ট পার্থক্যটা বুঝলেই পুরো ব্যাপারটা সহজ হয়ে যায়।

---

## ৬. High Availability — Cluster, Quorum Queue ও Raft

আরও বিস্তারিতভাবে বুঝিয়ে দিচ্ছি RabbitMQ-এর High Availability ব্যাপারটা।

### প্রথমে বুঝি সমস্যাটা কী

ধরুন আপনার একটামাত্র RabbitMQ সার্ভার (single node) চলছে। সেই সার্ভারটা যদি ক্র্যাশ করে (hardware fail, power outage, বা কেউ ভুল করে সার্ভার রিস্টার্ট দিলো), তাহলে:
- Queue-তে জমে থাকা সব মেসেজ হারিয়ে যেতে পারে
- Producer আর মেসেজ পাঠাতে পারবে না
- Consumer আর মেসেজ পড়তে পারবে না
- পুরো সিস্টেম সেই একটা সার্ভারের উপর নির্ভরশীল — এটাকে বলে **Single Point of Failure (SPOF)**

High Availability মানে হলো — এমন একটা ব্যবস্থা করা যাতে **একটা নোড ডাউন হলেও পুরো সিস্টেম চলতে থাকে, ডেটা হারায় না**।

### Cluster কীভাবে কাজ করে

RabbitMQ **Cluster** হলো একাধিক RabbitMQ নোড (সার্ভার) একসাথে যুক্ত করে একটা logical unit বানানো। যেমন ৩টা নোড (Node A, Node B, Node C) মিলে একটা cluster।

গুরুত্বপূর্ণ ব্যাপার: **Metadata (exchange, binding, user permission ইত্যাদি তথ্য) সব নোডে সবসময় sync হয়ে থাকে।** কিন্তু আসল মেসেজ ডেটা কোথায় থাকবে, সেটা নির্ভর করে আপনি কোন ধরনের Queue ব্যবহার করছেন তার উপর।

### Quorum Queue — বিস্তারিত

Quorum Queue হলো এমন এক ধরনের Queue যেখানে একটা Queue-এর ডেটা **একাধিক নোডে কপি (replica) হয়ে থাকে**। ধরুন ৩ নোডের cluster-এ একটা Quorum Queue বানালেন replication factor ৩ দিয়ে — তাহলে সেই Queue-এর একটা **leader** replica আর দুইটা **follower** replica থাকবে, প্রতিটা আলাদা নোডে।

#### Raft Consensus Algorithm — কীভাবে কাজ করে

এখানেই আসল ম্যাজিক। Raft হলো একটা algorithm যেটা নিশ্চিত করে সব replica-এর ডেটা **consistent** (একই রকম) থাকে।

**ধাপে ধাপে যা ঘটে:**

1. যখন একটা মেসেজ আসে, সেটা প্রথমে **Leader** replica-তে যায় (প্রতিটা Queue-এর একটাই leader থাকে)
2. Leader সেই মেসেজটা তার **Follower** replica গুলোর কাছে পাঠায়
3. **যখন majority (অর্ধেকের বেশি) follower** সেই মেসেজ নিজের কাছে সেভ করে ফেলার কনফার্মেশন পাঠায়, তখনই Leader ধরে নেয় মেসেজটা "committed" — মানে নিরাপদ
4. এরপরই Producer-কে জানানো হয় মেসেজ সফলভাবে গৃহীত হয়েছে

**কেন majority (অর্ধেকের বেশি) দরকার?** ৩ নোডের ক্ষেত্রে অন্তত ২টা নোডে (Leader + ১ Follower) ডেটা থাকলেই এটা "committed" ধরা হয়। এর মানে, ১টা নোড crash করলেও বাকি ২টাতে ডেটা আছে, তাই কিছু হারায় না।

### Leader Crash হলে কী হয়

এটাই সবচেয়ে গুরুত্বপূর্ণ অংশ। যদি Leader নোড হঠাৎ crash করে:

1. বাকি Follower নোডগুলো বুঝতে পারে Leader আর response দিচ্ছে না (heartbeat miss হচ্ছে)
2. Raft algorithm অনুযায়ী, বেঁচে থাকা নোডগুলোর মধ্যে একটা **নতুন Election** হয় — ভোটাভুটির মতো
3. যে নোডের কাছে সবচেয়ে **up-to-date ডেটা** আছে, সেটা নতুন Leader নির্বাচিত হয়
4. এই পুরো প্রক্রিয়াটা কয়েক সেকেন্ডের মধ্যেই শেষ হয়ে যায়, এবং Producer/Consumer নতুন Leader-এর সাথে আবার কানেক্ট করে কাজ চালিয়ে যায়

**গুরুত্বপূর্ণ**: যেহেতু নতুন Leader-এর কাছে already committed সব ডেটা আছে (majority replica-তে ছিল বলে), তাই কোনো মেসেজ হারায় না।

### Real World উদাহরণ

ধরুন একটা e-commerce কোম্পানি যাদের black Friday sale চলছে — হাজার হাজার অর্ডার প্রতি সেকেন্ডে আসছে। তাদের RabbitMQ যদি একটা মাত্র সার্ভারে চলে আর সেটা crash করে, পুরো sale-এর অর্ডার প্রসেসিং বন্ধ হয়ে যাবে — বিশাল রেভিনিউ লস।

এর বদলে তারা ৫ নোডের একটা cluster রাখে Quorum Queue দিয়ে। একটা নোডে hardware সমস্যা হলেও (যেমন disk fail), বাকি ৪টা নোড কাজ চালিয়ে যায় নিরবচ্ছিন্নভাবে — কাস্টমাররা কিছুই টের পায় না।

### একটা গুরুত্বপূর্ণ Trade-off (ইন্টারভিউতে জিজ্ঞেস করতে পারে)

Quorum Queue এর replication এর কারণে **write latency একটু বাড়ে** — কারণ প্রতিটা মেসেজ কমিট হওয়ার আগে majority নোডের কনফার্মেশন লাগে। অর্থাৎ **Availability আর Latency-এর মধ্যে একটা trade-off** থাকে — এটা একটা common interview follow-up প্রশ্ন, তাই মাথায় রাখা ভালো।

---

## ৭. Mirrored Queue — পুরনো ও Deprecated পদ্ধতি

Mirrored Queue নিয়ে বিস্তারিত বুঝিয়ে দিচ্ছি — এটা RabbitMQ-এর পুরনো High Availability পদ্ধতি, যেটা এখন **deprecated** (RabbitMQ 3.13 এ পুরোপুরি সরিয়ে ফেলা হয়েছে, Quorum Queue-ই এখন standard)।

### Mirrored Queue কীভাবে কাজ করতো

এটা Quorum Queue আসার আগে (RabbitMQ-এর পুরনো ভার্সনে) HA-এর জন্য ব্যবহৃত হতো। ধারণাটা সহজ:

- একটা Queue-এর একটা **Master** replica থাকতো একটা নোডে
- আর কয়েকটা **Mirror (Slave)** replica থাকতো অন্য নোডগুলোতে
- সব মেসেজ Master-এ আসতো, আর Master সেটা Mirror-গুলোতেও কপি করে পাঠাতো
- Master crash করলে একটা Mirror-কে নতুন Master বানানো হতো

শুনতে তো Quorum Queue-এর মতোই লাগছে, তাই না? কিন্তু এর ভেতরে বেশ কিছু গুরুতর সমস্যা ছিল, যেগুলোর কারণেই RabbitMQ টিম এটা বাদ দিয়ে Quorum Queue নিয়ে এসেছে।

### কেন Mirrored Queue বাদ দেওয়া হলো — মূল সমস্যাগুলো

#### ১. কোনো Consensus Algorithm ছিল না
Mirrored Queue-তে **Raft-এর মতো কোনো formal consensus protocol ছিল না**। Master মেসেজ Mirror-এ পাঠাতো ঠিকই, কিন্তু "majority নোড কনফার্ম করলো কিনা" — এই ধরনের কড়া guarantee ছিল না। ফলে কিছু edge case-এ (network split, একসাথে একাধিক নোড crash) ডেটা **সাইলেন্টলি হারিয়ে যেতে পারতো** বা দুইটা নোড নিজেকে Master ভাবতে পারতো (split-brain সমস্যা)।

#### ২. Split-Brain সমস্যা
নেটওয়ার্ক partition হলে (যেমন দুইটা নোডের মধ্যে সাময়িকভাবে যোগাযোগ বিচ্ছিন্ন হয়ে গেলো), দুই পাশই ভাবতে পারতো নিজেই আসল Master। নেটওয়ার্ক ফিরে এলে কোনটা "সঠিক" ডেটা সেটা ঠিক করা কঠিন হয়ে যেতো, আর ডেটা ইনকনসিস্টেন্সি তৈরি হতো।

#### ৩. Performance সমস্যা
Mirror synchronization প্রক্রিয়াটা বেশ heavyweight ছিল। Mirror-এর সংখ্যা বাড়ালে throughput কমে যেতো অনেকখানি, কারণ Master-কে প্রতিটা মেসেজ প্রতিটা Mirror-এ synchronously পাঠাতে হতো — কোনো efficient batching বা optimized replication protocol ছিল না।

#### ৪. Failover-এর সময় ডেটা হারানোর ঝুঁকি
Master crash করলে যে Mirror নতুন Master হতো, তার কাছে সবসময় ১০০% up-to-date ডেটা থাকার guarantee ছিল না (কারণ replication synchronous না-ও হতে পারতো কিছু configuration-এ)। তাই failover-এর সময় শেষ কিছু মেসেজ হারিয়ে যাওয়ার সম্ভাবনা থাকতো।

### Quorum Queue কীভাবে এই সমস্যাগুলো সমাধান করলো

| বিষয় | Mirrored Queue | Quorum Queue |
|---|---|---|
| Consensus | কোনো formal algorithm নেই | Raft algorithm (প্রমাণিত, ব্যাংকিং-গ্রেড consistency) |
| Split-brain | হতে পারতো | Raft-এর কারণে হয় না (majority vote লাগে) |
| Data safety | Majority confirm ছাড়াই commit হতে পারতো | Majority confirm না হলে commit হয় না |
| Performance | Mirror বাড়লে স্লো হতো বেশি | Optimized, বেশি stable throughput |
| Failover | Data loss risk ছিল | Guaranteed no data loss (committed data) |

### এক লাইনে মূল পার্থক্য

**Mirrored Queue** ছিল অনেকটা "copy-paste" পদ্ধতির মতো — Master যা করছে, সেটা অন্য নোডগুলোতে কপি করে দাও, কোনো formal ভোটাভুটি ছাড়াই।

**Quorum Queue** হলো "democratic voting" পদ্ধতির মতো — কোনো মেসেজকে "safe" ধরার আগে majority নোডের সম্মতি লাগবেই। এই ছোট্ট পার্থক্যটাই ডেটা সুরক্ষায় বিশাল তফাৎ তৈরি করে।

### Interview-এ যদি জিজ্ঞেস করে

**"আপনি কেন Mirrored Queue না ব্যবহার করে Quorum Queue ব্যবহার করবেন?"** — এর simple উত্তর: Mirrored Queue-তে formal consensus না থাকায় split-brain আর silent data loss-এর ঝুঁকি ছিল, আর এটা এখন RabbitMQ থেকে সম্পূর্ণ সরিয়েও ফেলা হয়েছে (৩.১৩ ভার্সন থেকে)। তাই নতুন যেকোনো প্রজেক্টে Quorum Queue (বা ছোট non-critical queue-এর জন্য সাধারণ Classic Queue) ব্যবহার করাই standard practice।

---

## ৮. RabbitMQ vs Kafka

RabbitMQ vs Kafka নিয়ে আরও গভীরে যাই — এটা একটা খুবই common এবং গুরুত্বপূর্ণ ইন্টারভিউ প্রশ্ন, তাই architecture-এর মূল পার্থক্যটা ভালোভাবে বুঝে নেওয়া দরকার।

### মূল আর্কিটেকচারাল পার্থক্য — এটাই আসল কারণ

#### RabbitMQ: Queue-based Model (Traditional Message Broker)

RabbitMQ-তে একটা মেসেজ যখন Queue-তে ঢোকে, এটা একটা **task হিসেবে ট্রিট হয়** — কেউ একজন সেটা নেবে, প্রসেস করবে, ack পাঠাবে, আর তারপর মেসেজটা **স্থায়ীভাবে মুছে যাবে**। এটা একবার consume হয়ে গেলে চিরতরে চলে যায়।

**এটা "Smart Broker, Dumb Consumer" মডেল** — মানে RabbitMQ নিজেই routing, priority, DLQ, retry এসবের জটিল লজিক হ্যান্ডেল করে, Consumer-এর কাজ শুধু মেসেজ নিয়ে process করা।

#### Kafka: Log-based Model (Distributed Log)

Kafka সম্পূর্ণ ভিন্নভাবে কাজ করে। এখানে মেসেজ (এটাকে বলে **event**) একটা **append-only log**-এ যোগ হয় — অনেকটা একটা খাতার মতো, যেখানে নতুন এন্ট্রি সবসময় শেষে যোগ হয়, আগের এন্ট্রি মোছা হয় না।

- প্রতিটা event-এর একটা **offset** (নাম্বার/position) থাকে
- Consumer নিজে ট্র্যাক রাখে সে কোন offset পর্যন্ত পড়েছে
- একই event **একাধিক independent Consumer Group** পড়তে পারে — একজন পড়ে ফেললেও মেসেজ মোছে না, তাই অন্যরাও পড়তে পারবে
- চাইলে offset পিছিয়ে দিয়ে **আগের ডেটা আবার replay** করা যায় (যেমন কোনো bug fix করার পর পুরনো ডেটা আবার প্রসেস করতে চাইলে)

**এটা "Dumb Broker, Smart Consumer" মডেল** — Kafka শুধু ডেটা store আর distribute করে, বাকি লজিক (কে কতটুকু পড়েছে, কীভাবে process করবে) Consumer নিজে সামলায়।

### কেন এই পার্থক্যটা Use Case নির্ধারণ করে

#### RabbitMQ কেন Payment/Order Processing-এর জন্য ভালো

1. **Exactly reflects task queue semantics**: একটা payment একবারই process হওয়া উচিত, একজন worker-ই সেটা নেবে — এটাই RabbitMQ-এর natural behavior
2. **Complex routing দরকার**: Priority queue (urgent payment আগে), DLQ (fail হলে আলাদা জায়গায়), TTL — এসব RabbitMQ-তে built-in এবং সহজ
3. **কম latency দরকার একটা single request-এর জন্য**: RabbitMQ push-based, তাই মেসেজ আসামাত্রই consumer-কে পাঠিয়ে দেয়

#### Kafka কেন Clickstream/Analytics-এর জন্য ভালো

1. **একই ডেটা multiple team ব্যবহার করে**: একটা user click event Analytics team, Fraud detection team, Recommendation team — সবাই আলাদাভাবে পড়বে। RabbitMQ-তে এটা করতে হলে Fanout exchange দিয়ে একই মেসেজ multiple Queue-তে কপি পাঠাতে হতো (storage duplicate হতো), Kafka-তে একই log সবাই শেয়ার করে
2. **বিশাল throughput**: Kafka disk-এ sequential write করে (অনেক দ্রুত), আর batching করে অনেক event একসাথে পাঠায় — তাই প্রতি সেকেন্ডে লক্ষ লক্ষ event handle করতে পারে
3. **Replay দরকার**: একটা নতুন ML model বানানোর সময় গত ৬ মাসের সব clickstream data আবার প্রসেস করতে চাইলে, Kafka থেকে সেই আগের offset থেকে আবার পড়া যায় — RabbitMQ-তে এটা সম্ভব না, কারণ মেসেজ already মুছে গেছে

### একটা Practical Comparison Table

| বিষয় | RabbitMQ | Kafka |
|---|---|---|
| মূল Use case | Task queue, RPC, complex routing | Event streaming, log aggregation |
| মেসেজ delete হয় কখন | Ack এর পর সাথে সাথে | Retention period (যেমন ৭ দিন) পর্যন্ত থাকে |
| Throughput | মাঝারি (হাজার-লক্ষ/সেকেন্ড) | অনেক বেশি (লক্ষ-কোটি/সেকেন্ড) |
| Replay করা যায় কি | না | হ্যাঁ, offset দিয়ে |
| Multiple consumer একই ডেটা পড়া | কঠিন (fanout দিয়ে duplicate করতে হয়) | Natural, built-in (consumer groups) |
| Priority/DLQ | Built-in সহজে | নিজে implement করতে হয় |
| Ordering | Per-queue (single consumer হলে) | Per-partition guaranteed |

### Real World: একই কোম্পানি দুটোই ব্যবহার করে

একটা বড় e-commerce কোম্পানি (Daraz টাইপ) সাধারণত **দুটোই একসাথে ব্যবহার করে**, কারণ তাদের দুই ধরনের প্রয়োজন আছে:

- **RabbitMQ**: অর্ডার placed হলে payment gateway-কে call করা, ইনভয়েস জেনারেট করা, SMS পাঠানো — এগুলো task-based, guaranteed exactly-once execution দরকার
- **Kafka**: ইউজার কোন প্রোডাক্ট দেখলো, কতক্ষণ দেখলো, কী সার্চ করলো — এই বিশাল পরিমাণ clickstream ডেটা সংগ্রহ করে Analytics, Recommendation Engine, আর Fraud Detection সিস্টেমে পাঠানো, যেখানে একই ডেটা একাধিক টিম আলাদাভাবে ব্যবহার করে

### Interview-এ যদি জিজ্ঞেস করে: "একটাই কেন বেছে নেবেন না?"

**সহজ উত্তর**: RabbitMQ দিয়ে Kafka-এর কাজ করতে গেলে (millions of events/sec, replay) সিস্টেম আটকে যাবে বা অস্বাভাবিক জটিল হয়ে যাবে। আবার Kafka দিয়ে simple task-queue কাজ (payment processing with priority+DLQ) করতে গেলে অনেক বেশি বাড়তি কোড লিখতে হবে, যেটা RabbitMQ-তে built-in সুবিধা হিসেবেই পাওয়া যায়। তাই **"right tool for the right job"** — এটাই আসল উত্তর।

---

## ৯. ইন্টারভিউ প্রশ্ন — সব লেভেল

RabbitMQ ইন্টারভিউতে সাধারণত এই ধরনের প্রশ্ন আসে, লেভেল অনুযায়ী ভাগ করে দিলাম:

### Basic Conceptual Questions
- RabbitMQ কী, এবং message broker কীভাবে কাজ করে?
- RabbitMQ vs Kafka — পার্থক্য কী? কোনটা কখন ব্যবহার করবেন?
- Message Queue ব্যবহার করার সুবিধা কী? (synchronous vs asynchronous communication)
- **Producer, Consumer, Queue, Exchange, Binding** — এগুলো কী এবং কীভাবে একসাথে কাজ করে?
- AMQP protocol কী?

### Exchange Types নিয়ে (খুব common)
- Exchange কত ধরনের হয়? (Direct, Fanout, Topic, Headers) — প্রতিটার difference এবং use case বলতে বলবে
- উদাহরণস্বরূপ জিজ্ঞেস করতে পারে: "আপনি যদি সব consumer-কে একই মেসেজ broadcast করতে চান, কোন exchange ব্যবহার করবেন?" (উত্তর: Fanout)
- routing key কীভাবে কাজ করে Direct আর Topic exchange-এ?

### Reliability & Delivery Guarantees
- Message কীভাবে guarantee করবেন যে হারিয়ে যাবে না? (Persistent messages, durable queues)
- **Acknowledgement (ack/nack)** কীভাবে কাজ করে? Manual vs automatic ack?
- Message যদি process করতে গিয়ে consumer crash করে, তাহলে কী হয়?
- **Dead Letter Queue (DLQ)** কী এবং কেন দরকার?
- Idempotency নিয়ে প্রশ্ন — একই মেসেজ দুইবার process হলে কীভাবে handle করবেন?

### Performance & Scaling
- একটা queue-তে অনেক consumer থাকলে load কীভাবে distribute হয়?
- Prefetch count কী এবং কেন ইম্পরট্যান্ট?
- High throughput scenario-তে RabbitMQ কীভাবে scale করবেন? (Clustering, sharding queues)
- Message TTL (Time To Live) কী?

### Practical/Scenario-based Questions
এগুলো বেশি আসে experienced position-এর জন্য:
- "একটা অর্ডার প্রসেসিং সিস্টেম ডিজাইন করুন যেখানে RabbitMQ ব্যবহার হবে" — এই ধরনের system design প্রশ্ন
- "যদি একটা consumer বারবার fail করে একটা মেসেজ process করতে, তাহলে infinite loop এড়াতে কীভাবে handle করবেন?" (Retry limit + DLQ)
- "কীভাবে নিশ্চিত করবেন যে দুইটা মেসেজ একই order-এ process হবে?"
- Priority Queue কীভাবে implement করবেন?

### Comparison Questions
- RabbitMQ vs Kafka vs Redis Pub/Sub — কোনটা কখন?
  - সংক্ষেপে বলি: **RabbitMQ** — complex routing, guaranteed delivery দরকার হলে ভালো। **Kafka** — high throughput, event streaming, log-based processing দরকার হলে ভালো। **Redis Pub/Sub** — simple, fast, কিন্তু persistence নাই (মেসেজ miss হলে চলে যায়)

### কোড/Implementation Level (যদি hands-on round থাকে)
- আপনার পছন্দের language-এ (Node.js/Python/Java) RabbitMQ producer-consumer লিখতে বলতে পারে
- Connection vs Channel — পার্থক্য কী?
- Error handling কীভাবে করবেন consumer side-এ?

---

## ১০. Advanced ইন্টারভিউ প্রশ্ন-উত্তর

আরও কিছু ইন্টারভিউ প্রশ্ন-উত্তর, এবার একটু advanced এবং practical scenario নিয়ে:

### Q1: RabbitMQ কীভাবে High Availability নিশ্চিত করে?
**উত্তর**: RabbitMQ **Cluster** বানানো যায় — একাধিক নোড একসাথে কাজ করে। একটা নোড ডাউন হলেও বাকি নোড কাজ চালিয়ে যায়। Queue-এর data একাধিক নোডে রাখার জন্য **Quorum Queue** (আগে ছিল Mirrored Queue, এখন deprecated) ব্যবহার হয় — এটা Raft consensus algorithm দিয়ে কাজ করে, তাই একটা নোড crash করলেও ডেটা হারায় না।

### Q2: দুইটা মেসেজ একই order-এ process হবে, এটা কীভাবে নিশ্চিত করবেন?
**উত্তর**: একটা Queue-তে যদি **একটাই Consumer** থাকে, তাহলে মেসেজ FIFO order-এ আসে। কিন্তু multiple consumer থাকলে order guarantee থাকে না, কারণ একেকটা মেসেজ একেক গতিতে process হয়। Order দরকার হলে সমাধান: একটা related মেসেজ group কে একই Consumer-এর কাছে পাঠানো (Consistent hashing exchange ব্যবহার করে), অথবা single consumer রেখে ভেতরে queue বানানো।

**Real world**: ব্যাংকের transaction system-এ একই account-এর সব transaction অবশ্যই order মেনে process হতে হবে (deposit-এর আগে withdraw হলে সমস্যা), তাই account ID অনুযায়ী routing key ঠিক করে একই queue/consumer-এ পাঠানো হয়।

### Q3: Consumer বারবার একই মেসেজ process করে fail করছে — infinite retry কীভাবে আটকাবেন?
**উত্তর**: একটা **retry counter** header-এ রাখা হয় মেসেজের সাথে। প্রতিবার fail হলে counter বাড়ে। একটা limit (যেমন ৩ বার) পার হয়ে গেলে মেসেজটা আর requeue না করে **DLQ**-তে পাঠিয়ে দেওয়া হয়, এবং alert পাঠানো হয় যাতে মানুষ ম্যানুয়ালি দেখতে পারে।

### Q4: RabbitMQ vs Kafka — কোনটা কখন বেছে নেবেন? (real scenario দিয়ে)
- **RabbitMQ**: ব্যাংকের payment processing, অর্ডার প্রসেসিং — যেখানে প্রতিটা মেসেজ নির্দিষ্ট একজন consumer process করবে, guaranteed delivery দরকার, আর complex routing (priority, DLQ) দরকার।
- **Kafka**: লক্ষ লক্ষ ইউজারের clickstream/activity log, বা IoT sensor data — যেখানে বিশাল throughput দরকার, একই ডেটা একাধিক consumer (analytics, fraud detection, recommendation) একসাথে পড়বে, আর পুরনো ডেটা replay করার দরকার হতে পারে।

### Q5: RPC pattern RabbitMQ দিয়ে কীভাবে implement করবেন?
**উত্তর**: সাধারণত RabbitMQ fire-and-forget (async), কিন্তু sometimes response দরকার হয় (যেমন request-response)। এর জন্য:
- Producer একটা মেসেজ পাঠায় সাথে একটা `reply_to` queue name আর `correlation_id` দিয়ে
- Consumer কাজ শেষ করে সেই `reply_to` queue-তে result পাঠায় একই `correlation_id` সহ
- Producer সেই correlation_id দিয়ে match করে বুঝে নেয় কোন request-এর response এলো

**Real world**: Microservices architecture-এ, যেমন Order Service, Payment Service-কে জিজ্ঞেস করে "এই কার্ডে টাকা আছে কিনা" এবং সরাসরি response wait করে (synchronous-এর মতো আচরণ, কিন্তু আসলে queue দিয়ে হচ্ছে)।

### Q6: Message Priority কীভাবে হ্যান্ডেল করবেন?
**উত্তর**: Queue declare করার সময় `x-max-priority` সেট করে priority queue বানানো যায় (০-১০)। মেসেজ পাঠানোর সময় `priority` ফিল্ড সেট করলে বেশি priority-র মেসেজ আগে process হয়।

**Real world**: Hospital-এর emergency notification system — critical alert (patient-এর vital sign খারাপ) সবসময় normal routine notification-এর আগে যাবে।

### Q7: একটা Queue-তে হঠাৎ মেসেজ জমে যাচ্ছে (backlog বাড়ছে) — কীভাবে handle করবেন?
**উত্তর**:
- **Horizontal scaling**: আরও consumer instance যোগ করা (competing consumers pattern)
- Consumer-এর processing logic optimize করা
- **Alerting** সেট করা যাতে queue length একটা threshold ছাড়ালে জানানো হয়
- দরকার হলে **auto-scaling** — queue length monitor করে dynamically worker বাড়ানো-কমানো (যেমন Kubernetes HPA দিয়ে)

### Q8: Idempotency কেন দরকার এবং কীভাবে implement করবেন?
**উত্তর**: RabbitMQ "at-least-once delivery" guarantee দেয় — মানে একই মেসেজ দুইবার (বা তার বেশি) deliver হতে পারে (যেমন consumer ack পাঠানোর ঠিক আগে crash করলে)। তাই Consumer-এর কাজ **idempotent** হতে হবে — একই মেসেজ দুইবার এলেও ফলাফল একই থাকবে।

**Real world**: Payment processing-এ — একই "charge_user" মেসেজ দুইবার এলে যেন ইউজারের কার্ড থেকে দুইবার টাকা না কাটে। সমাধান: প্রতিটা মেসেজের সাথে একটা unique transaction ID পাঠানো, এবং process করার আগে check করা এই ID আগে process হয়েছে কিনা (database-এ record রেখে)।

---

## ১১. আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q9–Q22)

এই প্রশ্নগুলো mid থেকে senior লেভেলের ইন্টারভিউতে বেশি আসে — architecture, edge case আর operational দিক নিয়ে।

### Q9: Virtual Host (vhost) কী এবং কেন দরকার?
**উত্তর**: vhost হলো একটা RabbitMQ সার্ভারের ভেতরে **logical isolation** — প্রতিটা vhost-এর নিজস্ব আলাদা exchange, queue, binding আর permission থাকে। একটা vhost-এর queue অন্য vhost থেকে দেখা যায় না।

**কেন দরকার**: একই RabbitMQ cluster-এ multiple team বা multiple environment (dev/staging/prod) চালাতে চাইলে vhost দিয়ে আলাদা করা হয় — যাতে একটার queue আরেকটার সাথে conflict না করে। যেমন `/payments` vhost আর `/notifications` vhost সম্পূর্ণ আলাদা।

### Q10: Publisher Confirms আর Transactions — পার্থক্য কী? কোনটা ব্যবহার করবেন?
**উত্তর**:
- **Transactions (`tx.select`/`tx.commit`)**: প্রতিটা publish একটা transaction-এ মোড়ানো হয়, commit না করা পর্যন্ত মেসেজ pending থাকে। এটা **খুব slow** (প্রতিটা commit-এ round-trip লাগে)।
- **Publisher Confirms**: Producer async-ভাবে publish করতে থাকে, RabbitMQ প্রতিটা মেসেজের জন্য পরে একটা `ack` (বা `nack`) পাঠায়। অনেক দ্রুত, কারণ batch/pipeline করা যায়।

**Best practice**: reliability দরকার হলে Publisher Confirms ব্যবহার করুন, transaction নয় — ১০x+ বেশি throughput পাওয়া যায়।

### Q11: Message TTL — per-queue vs per-message পার্থক্য কী?
**উত্তর**:
- **Per-queue TTL** (`x-message-ttl` queue argument): ওই queue-এর সব মেসেজের জন্য একই expiry।
- **Per-message TTL** (`expiration` property): প্রতিটা মেসেজে আলাদা করে সময় সেট করা যায়।

**সূক্ষ্ম ব্যাপার**: RabbitMQ শুধু queue-এর **head**-এর মেসেজ expire হয়েছে কিনা চেক করে। তাই per-message TTL-এ পেছনের মেসেজ আগে expire হলেও, সামনের মেসেজ না সরা পর্যন্ত সেটা delete হয় না — এটা interview-এ tricky follow-up।

### Q12: Delayed / Scheduled message কীভাবে পাঠাবেন? (যেমন "৩০ মিনিট পর reminder")
**উত্তর**: দুইটা উপায়:
1. **Dead Letter + TTL trick**: একটা "delay queue" বানান যার consumer নেই, `x-message-ttl` সেট করা, আর `x-dead-letter-exchange` মূল queue-এ point করা। মেসেজ TTL শেষে DLX দিয়ে আসল queue-তে চলে আসে।
2. **`rabbitmq-delayed-message-exchange` plugin**: এটাই cleaner — মেসেজে `x-delay` header দিলে exchange নিজেই ওই সময় পর্যন্ত ধরে রাখে।

**Real world**: abandoned cart email (৩০ মিনিট পর), subscription renewal reminder, retry with backoff।

### Q13: Exactly-once delivery কি RabbitMQ দিয়ে সম্ভব?
**উত্তর**: কড়া অর্থে **না**। RabbitMQ **at-least-once** দেয় (network fail, redelivery-র কারণে duplicate আসতে পারে)। "Exactly-once processing" বাস্তবে অর্জন করা হয় **at-least-once delivery + idempotent consumer** দিয়ে — অর্থাৎ duplicate এলেও consumer একই ফল দেয় (dedup key/DB check দিয়ে)। এটা একটা খুব common "gotcha" প্রশ্ন।

### Q14: `basic.reject` আর `basic.nack` — পার্থক্য কী?
**উত্তর**: দুটোই মেসেজ reject করে, কিন্তু:
- `basic.reject` — একবারে **একটা** মেসেজ reject করতে পারে।
- `basic.nack` — RabbitMQ-এর extension, `multiple: true` দিয়ে **একসাথে অনেক** মেসেজ reject করা যায়।

দুটোতেই `requeue` ফ্ল্যাগ আছে — `requeue=false` দিলে মেসেজ DLQ-তে যায় (থাকলে), নাহলে drop হয়।

### Q15: Prefetch-এ `global` flag-এর মানে কী?
**উত্তর**: `basic.qos`-এ prefetch count দুইভাবে কাজ করে:
- **per-consumer** (default): প্রতিটা consumer আলাদাভাবে সর্বোচ্চ N মেসেজ পায়।
- **global=true**: পুরো **channel**-এর জন্য সম্মিলিতভাবে সর্বোচ্চ N।

সাধারণত per-consumer prefetch-ই চাই, যাতে fast consumer বেশি কাজ পায় আর slow consumer কম — **fair dispatch**।

### Q16: Connection ছিঁড়ে গেলে কী হয়? Automatic recovery কীভাবে কাজ করে?
**উত্তর**: Network glitch-এ connection/channel বন্ধ হয়ে যেতে পারে। বেশিরভাগ client library-তে **automatic connection recovery** থাকে — connection ফিরে এলে channel, queue, binding, consumer আবার নিজে থেকে declare করে নেয়। সাথে **heartbeat** (default ৬০ সেকেন্ড) দিয়ে RabbitMQ আর client পরস্পরকে "জীবিত আছি" জানায়; heartbeat miss হলে connection dead ধরে নেওয়া হয়।

**গুরুত্বপূর্ণ**: recovery-র সময় unacked মেসেজ redeliver হবে — তাই আবারও consumer idempotent হওয়া লাগে।

### Q17: RabbitMQ-তে Memory / Disk alarm কী?
**উত্তর**: RabbitMQ একটা **flow control** ব্যবস্থা — যখন RAM ব্যবহার একটা threshold (`vm_memory_high_watermark`, default ৪০%) ছাড়ায় বা free disk কমে যায়, তখন সে **publisher-দের block** করে দেয় (নতুন মেসেজ নেওয়া থামিয়ে দেয়), যাতে সার্ভার crash না করে। Consumer কাজ চালিয়ে যায়, backlog কমলে আবার publisher খুলে যায়। Production issue debug করতে এটা জানা জরুরি।

### Q18: Quorum Queue আর Classic Queue — কখন কোনটা?
**উত্তর**:
- **Quorum Queue**: data safety + HA দরকার (payment, order) — Raft দিয়ে replicated, no data loss।
- **Classic Queue**: non-critical, ephemeral, বা খুব high-throughput temporary কাজ (যেমন per-client RPC reply queue) — হালকা, কিন্তু single-node, replicate হয় না।

Mirrored (HA classic) queue এখন deprecated — নতুন প্রজেক্টে HA লাগলে Quorum।

### Q19: Competing Consumers আর Pub/Sub pattern-এর পার্থক্য RabbitMQ-তে কীভাবে হয়?
**উত্তর**:
- **Competing Consumers (work queue)**: একটা queue, অনেক consumer — প্রতিটা মেসেজ **একজনই** পায় (load sharing)। Default direct/queue behavior।
- **Pub/Sub (fanout)**: fanout exchange-এ একাধিক queue bind করা, প্রতিটা queue-র নিজস্ব consumer — একই মেসেজ **সবাই** পায় (broadcast)।

মূল কৌশল: "মেসেজ একজন নেবে" চাইলে এক queue শেয়ার করান; "সবাই নেবে" চাইলে প্রত্যেকের আলাদা queue বানান।

### Q20: Shovel আর Federation plugin কী কাজে লাগে?
**উত্তর**: দুটোই **broker-to-broker** মেসেজ move করার জন্য (যেমন এক datacenter থেকে আরেকটায়):
- **Shovel**: এক queue থেকে মেসেজ টেনে অন্য broker-এর exchange/queue-তে পাঠায় — point-to-point, সহজ কনফিগ।
- **Federation**: exchange/queue level-এ link — একাধিক broker-জুড়ে মেসেজ শেয়ার, WAN-friendly (loose coupling)।

**Use case**: multi-region deployment, on-prem থেকে cloud-এ migration, geo-distributed system।

### Q21: Poison message কী এবং কীভাবে handle করবেন?
**উত্তর**: যে মেসেজ কখনোই সফলভাবে process হয় না (malformed data, permanent bug) — বারবার fail করে requeue হয়ে queue আটকে দেয়, এটাই **poison message**। সমাধান: `x-death` header দিয়ে retry count track করা, নির্দিষ্ট সংখ্যক fail-এর পর **DLQ**-তে সরিয়ে দেওয়া এবং alert তোলা — মূল pipeline সচল রাখা।

### Q22: RabbitMQ কীভাবে monitor করবেন production-এ?
**উত্তর**:
- **Management Plugin** (web UI + HTTP API): queue depth, message rate, consumer count, memory।
- **Prometheus + Grafana**: `rabbitmq_prometheus` plugin দিয়ে metrics scrape করে dashboard/alert।
- মূল যে metric-গুলো watch করবেন: **queue length (backlog)**, **unacked message count**, **consumer utilisation**, **memory/disk alarm**, **redelivery rate**।

---

## ১২. Real-World Project — সমস্যা ও সমাধান (কোডসহ)

এই সেকশনে কয়েকটা বাস্তব প্রজেক্ট সমস্যা, আর RabbitMQ দিয়ে কীভাবে সেগুলো সমাধান করা হয় — practical কোড (Node.js `amqplib` / Python `pika`) সহ দেখানো হলো।

### সমস্যা ১: Sign-up slow — সব কাজ synchronously হচ্ছে

**সমস্যা**: User sign up করলে welcome email, SMS, CRM sync — সব একসাথে হয়, ফলে API response নিতে ৩–৫ সেকেন্ড লাগে।

**সমাধান**: Sign-up হওয়ামাত্র শুধু একটা event publish করে সাথে সাথে response দিন; বাকি কাজ background consumer করবে।

```js
// producer — signup API (Node.js, amqplib)
const amqp = require('amqplib');

async function publishUserRegistered(user) {
  const conn = await amqp.connect('amqp://localhost');
  const ch = await conn.createConfirmChannel();          // publisher confirms
  await ch.assertExchange('user.events', 'topic', { durable: true });

  ch.publish(
    'user.events',
    'user.registered',
    Buffer.from(JSON.stringify({ userId: user.id, email: user.email })),
    { persistent: true, messageId: `signup-${user.id}` }  // durable + dedup id
  );
  await ch.waitForConfirms();   // RabbitMQ সত্যিই পেয়েছে কিনা নিশ্চিত হই
  await ch.close(); await conn.close();
}
// API এখন ~50ms-এ "success" ফেরত দেয়, বাকি কাজ async
```

```js
// consumer — email worker
const ch = await conn.createChannel();
await ch.assertQueue('email.welcome', { durable: true });
await ch.bindQueue('email.welcome', 'user.events', 'user.registered');
ch.prefetch(20);                                   // fair dispatch
ch.consume('email.welcome', async (msg) => {
  const { email } = JSON.parse(msg.content.toString());
  try {
    await sendWelcomeEmail(email);
    ch.ack(msg);                                   // manual ack — কাজ শেষে
  } catch (err) {
    ch.nack(msg, false, false);                    // fail → DLQ-তে যাক
  }
});
```

### সমস্যা ২: Payment webhook দুইবার এসে দুইবার টাকা কাটছে (Idempotency)

**সমস্যা**: Redelivery-র কারণে একই `charge` মেসেজ দুইবার process হয়ে ইউজারের কার্ড থেকে দুইবার টাকা কাটে।

**সমাধান**: প্রতিটা মেসেজে unique key, আর DB-তে "processed keys" রেখে duplicate আটকান।

```python
# consumer (Python, pika) — idempotent payment worker
def on_message(ch, method, props, body):
    data = json.loads(body)
    txn_id = data["transaction_id"]        # unique per payment attempt

    # আগে process হয়েছে কিনা — atomic insert দিয়ে চেক
    if db.already_processed(txn_id):
        ch.basic_ack(method.delivery_tag)  # duplicate — চুপচাপ ack করে ফেলে দাও
        return
    try:
        charge_card(data["amount"], data["card"])
        db.mark_processed(txn_id)          # একই transaction-এ commit
        ch.basic_ack(method.delivery_tag)
    except TransientError:
        ch.basic_nack(method.delivery_tag, requeue=True)   # পরে আবার try
    except PermanentError:
        ch.basic_nack(method.delivery_tag, requeue=False)  # DLQ-তে
```

### সমস্যা ৩: ব্যাংক transaction-এর order উল্টে যাচ্ছে

**সমস্যা**: Multiple consumer থাকায় একই account-এর `deposit`-এর আগে `withdraw` process হয়ে balance ভুল হয়।

**সমাধান**: **Consistent Hashing Exchange** দিয়ে একই `account_id`-এর সব মেসেজ একই queue → একই consumer-এ পাঠান।

```js
// consistent-hash exchange: routing key = account_id
await ch.assertExchange('txn', 'x-consistent-hash', { durable: true });

// প্রতিটা shard queue আলাদা consumer, ভেতরে order ঠিক থাকে
for (let i = 0; i < 10; i++) {
  const q = `txn.shard.${i}`;
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, 'txn', '1');       // weight
}

// publish — একই account সবসময় একই shard-এ
ch.publish('txn', accountId, Buffer.from(JSON.stringify(txn)), { persistent: true });
```

> মূল নীতি: **global order নয়, per-entity order** — একই account-এর মেসেজ একই লাইনে।

### সমস্যা ৪: Newsletter — লক্ষ ইমেইলে মূল অ্যাপ আটকে যাচ্ছে

**সমস্যা**: ১০ লক্ষ ইউজারকে newsletter পাঠাতে গেলে অ্যাপ block হয়ে যায়, provider rate-limit-ও hit করে।

**সমাধান**: Work queue + অনেক competing consumer + prefetch দিয়ে throughput নিয়ন্ত্রণ। Producer শুধু জব push করে, worker pool ধীরে ধীরে খায়।

```js
await ch.assertQueue('email.bulk', {
  durable: true,
  arguments: { 'x-max-length': 5_000_000 }   // backlog সীমা
});
// N worker pod, প্রত্যেকে prefetch(50) — provider rate অনুযায়ী tune
ch.prefetch(50);
ch.consume('email.bulk', async (msg) => {
  await emailProvider.send(JSON.parse(msg.content));
  ch.ack(msg);
});
// throughput বাড়াতে শুধু worker pod সংখ্যা বাড়ান — অ্যাপ কোড বদলাতে হয় না
```

### সমস্যা ৫: Video upload — heavy processing-এ ইউজার wait করছে

**সমস্যা**: ভিডিও compress + thumbnail + multi-resolution convert-এ কয়েক মিনিট লাগে; upload response আটকে থাকে।

**সমাধান**: Upload হওয়ামাত্র একটা "transcode job" queue-তে দিন, ইউজারকে সাথে সাথে "processing…" দেখান। GPU worker background-এ কাজ করে, শেষে আরেকটা event দিয়ে status update করে।

```python
# upload handler — সাথে সাথে ফেরত দেয়
channel.basic_publish(
    exchange='media',
    routing_key='video.transcode',
    body=json.dumps({"video_id": vid, "path": s3_path}),
    properties=pika.BasicProperties(delivery_mode=2, priority=5),  # persistent
)
# heavy transcode worker আলাদা মেশিনে চলে, prefetch=1 (একেকটা job ভারী)
```

### সমস্যা ৬: Third-party API rate-limit + retry with backoff

**সমস্যা**: External SMS/API প্রতি সেকেন্ডে সীমিত কল নেয়; fail হলে সাথে সাথে retry করলে আরও fail হয়।

**সমাধান**: **Delay queue (TTL + DLX)** দিয়ে exponential backoff retry — fail হলে মেসেজ একটা wait queue-তে যায়, TTL শেষে মূল queue-তে ফিরে আসে; retry count DLQ threshold ঠিক করে।

```js
// wait queue: এখানে কোনো consumer নেই, TTL শেষে DLX দিয়ে ফেরত পাঠায়
await ch.assertQueue('sms.retry.30s', {
  durable: true,
  arguments: {
    'x-message-ttl': 30000,
    'x-dead-letter-exchange': '',            // default exchange
    'x-dead-letter-routing-key': 'sms.send', // ফিরে মূল queue-তে
  },
});

// consumer: fail হলে retry count দেখে সিদ্ধান্ত
ch.consume('sms.send', async (msg) => {
  const retries = (msg.properties.headers?.['x-retry'] || 0);
  try {
    await sendSms(JSON.parse(msg.content));
    ch.ack(msg);
  } catch (e) {
    if (retries >= 5) { ch.nack(msg, false, false); }     // DLQ → alert
    else {
      ch.sendToQueue('sms.retry.30s', msg.content, {
        persistent: true, headers: { 'x-retry': retries + 1 },
      });
      ch.ack(msg);                                         // পুরনোটা সরাও
    }
  }
});
```

### সমস্যা ৭: Order Service → Payment Service synchronous উত্তর দরকার (RPC)

**সমস্যা**: Order দেওয়ার আগে Payment Service-কে "কার্ডে টাকা আছে কিনা" জিজ্ঞেস করে উত্তরের জন্য অপেক্ষা করতে হয়।

**সমাধান**: RabbitMQ **RPC pattern** — `reply_to` + `correlation_id` দিয়ে request-response।

```python
# requester (Order Service)
corr_id = str(uuid.uuid4())
callback_q = channel.queue_declare('', exclusive=True).method.queue  # temp reply queue
channel.basic_publish(
    exchange='', routing_key='payment.check',
    properties=pika.BasicProperties(reply_to=callback_q, correlation_id=corr_id),
    body=json.dumps({"card": card, "amount": amount}),
)
# callback_q-তে correlation_id মিলিয়ে response ধরা হয় → sync-এর মতো আচরণ
```

### সমস্যা ৮: E-commerce microservices — একই order event অনেক টিম লাগবে

**সমস্যা**: Order placed হলে Inventory, Invoice, Notification, Analytics — সবাইকে জানাতে হবে, কিন্তু কেউ কারো উপর নির্ভর করবে না।

**সমাধান**: **Fanout / Topic exchange** দিয়ে event broadcast — প্রতিটা service-এর নিজস্ব queue, একই event সবাই স্বাধীনভাবে পায় ও process করে।

```js
await ch.assertExchange('order.events', 'fanout', { durable: true });

// প্রতিটা service নিজের durable queue bind করে — একজন slow হলেও বাকিরা চলে
for (const svc of ['inventory', 'invoice', 'notify', 'analytics']) {
  await ch.assertQueue(`order.${svc}`, { durable: true });
  await ch.bindQueue(`order.${svc}`, 'order.events', '');
}
// order placed → একবার publish, চারটা service আলাদাভাবে পায়
ch.publish('order.events', '', Buffer.from(JSON.stringify(order)), { persistent: true });
```

> **সারমর্ম**: প্রায় সব প্যাটার্নের মূল কথা একটাই — **কাজটাকে queue-তে ফেলে দাও, মূল request দ্রুত ছেড়ে দাও, আর background worker নিজের গতিতে নিরাপদে (durable + ack + retry + DLQ) কাজ শেষ করুক।**
