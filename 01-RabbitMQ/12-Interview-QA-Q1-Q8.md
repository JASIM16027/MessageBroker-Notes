# Advanced ইন্টারভিউ প্রশ্ন-উত্তর (Q1–Q8)


আরও কিছু ইন্টারভিউ প্রশ্ন-উত্তর, এবার একটু advanced এবং practical scenario নিয়ে:

### Q1: RabbitMQ কীভাবে High Availability নিশ্চিত করে?
**উত্তর**: RabbitMQ **Cluster** বানানো যায় — একাধিক নোড একসাথে কাজ করে। একটা নোড ডাউন হলেও বাকি নোড কাজ চালিয়ে যায়। Queue-এর data একাধিক নোডে রাখার জন্য **Quorum Queue** (আগে ছিল Mirrored Queue, এখন deprecated) ব্যবহার হয় — এটা Raft consensus algorithm দিয়ে কাজ করে, তাই একটা নোড crash করলেও ডেটা হারায় না।

```js
// HA-এর মূল চাবি: queue-টা quorum type-এ declare করা — cluster-জুড়ে replicate হবে
await ch.assertQueue('orders', {
  durable: true,
  arguments: { 'x-queue-type': 'quorum' }   // classic নয়, quorum → Raft replicated
});
// একই connection URL-এ একাধিক নোড দিলে একটা নোড ডাউন হলে client পরের নোডে যায়
// amqp.connect(['amqp://node1', 'amqp://node2', 'amqp://node3'])
```

### Q2: দুইটা মেসেজ একই order-এ process হবে, এটা কীভাবে নিশ্চিত করবেন?
**উত্তর**: একটা Queue-তে যদি **একটাই Consumer** থাকে, তাহলে মেসেজ FIFO order-এ আসে। কিন্তু multiple consumer থাকলে order guarantee থাকে না, কারণ একেকটা মেসেজ একেক গতিতে process হয়। Order দরকার হলে সমাধান: একটা related মেসেজ group কে একই Consumer-এর কাছে পাঠানো (Consistent hashing exchange ব্যবহার করে), অথবা single consumer রেখে ভেতরে queue বানানো।

**Real world**: ব্যাংকের transaction system-এ একই account-এর সব transaction অবশ্যই order মেনে process হতে হবে (deposit-এর আগে withdraw হলে সমস্যা), তাই account ID অনুযায়ী routing key ঠিক করে একই queue/consumer-এ পাঠানো হয়।

```js
// উপায় ১: একই account = একই routing key → consistent-hash exchange একই queue-তে পাঠায়
ch.publish('tx.hash', accountId, Buffer.from(data));   // accountId = routing key

// উপায় ২: Single Active Consumer — অনেক consumer bind, কিন্তু একসাথে একজনই active
await ch.assertQueue('account.tx', {
  durable: true,
  arguments: { 'x-single-active-consumer': true }   // order রক্ষা + auto-failover
});
```

### Q3: Consumer বারবার একই মেসেজ process করে fail করছে — infinite retry কীভাবে আটকাবেন?
**উত্তর**: একটা **retry counter** header-এ রাখা হয় মেসেজের সাথে। প্রতিবার fail হলে counter বাড়ে। একটা limit (যেমন ৩ বার) পার হয়ে গেলে মেসেজটা আর requeue না করে **DLQ**-তে পাঠিয়ে দেওয়া হয়, এবং alert পাঠানো হয় যাতে মানুষ ম্যানুয়ালি দেখতে পারে।

```js
ch.consume('tasks', async (msg) => {
  const retries = (msg.properties.headers?.['x-retry'] || 0);
  try {
    await doWork(msg);
    ch.ack(msg);
  } catch (err) {
    if (retries >= 3) {
      ch.nack(msg, false, false);          // requeue=false → DLQ-তে যাবে, alert তোলো
    } else {
      // counter বাড়িয়ে আবার publish, তারপর original-টা drop
      ch.publish('', 'tasks', msg.content, { headers: { 'x-retry': retries + 1 } });
      ch.ack(msg);
    }
  }
});
```

### Q4: RabbitMQ vs Kafka — কোনটা কখন বেছে নেবেন? (real scenario দিয়ে)
- **RabbitMQ**: ব্যাংকের payment processing, অর্ডার প্রসেসিং — যেখানে প্রতিটা মেসেজ নির্দিষ্ট একজন consumer process করবে, guaranteed delivery দরকার, আর complex routing (priority, DLQ) দরকার।
- **Kafka**: লক্ষ লক্ষ ইউজারের clickstream/activity log, বা IoT sensor data — যেখানে বিশাল throughput দরকার, একই ডেটা একাধিক consumer (analytics, fraud detection, recommendation) একসাথে পড়বে, আর পুরনো ডেটা replay করার দরকার হতে পারে।

```js
// RabbitMQ-এর জোর: complex routing/priority/DLQ কয়েক লাইনেই built-in
await ch.assertQueue('payments', {
  durable: true,
  arguments: {
    'x-max-priority': 10,                       // urgent payment আগে
    'x-dead-letter-exchange': 'payments.dlx'    // fail হলে DLQ
  }
});

// Kafka-তে (kafkajs) মূল পার্থক্য: মেসেজ delete হয় না, consumer offset ট্র্যাক করে replay
// await consumer.subscribe({ topic: 'clicks', fromBeginning: true }); // পুরনো ডেটা আবার পড়া
```

### Q5: RPC pattern RabbitMQ দিয়ে কীভাবে implement করবেন?
**উত্তর**: সাধারণত RabbitMQ fire-and-forget (async), কিন্তু sometimes response দরকার হয় (যেমন request-response)। এর জন্য:
- Producer একটা মেসেজ পাঠায় সাথে একটা `reply_to` queue name আর `correlation_id` দিয়ে
- Consumer কাজ শেষ করে সেই `reply_to` queue-তে result পাঠায় একই `correlation_id` সহ
- Producer সেই correlation_id দিয়ে match করে বুঝে নেয় কোন request-এর response এলো

**Real world**: Microservices architecture-এ, যেমন Order Service, Payment Service-কে জিজ্ঞেস করে "এই কার্ডে টাকা আছে কিনা" এবং সরাসরি response wait করে (synchronous-এর মতো আচরণ, কিন্তু আসলে queue দিয়ে হচ্ছে)।

```js
// ── Client (Order Service): request পাঠায়, correlationId দিয়ে reply match করে ──
const { queue: replyTo } = await ch.assertQueue('', { exclusive: true }); // temp reply queue
const correlationId = randomUUID();
ch.consume(replyTo, (msg) => {
  if (msg.properties.correlationId === correlationId) {
    console.log('উত্তর এলো:', msg.content.toString());   // এইটাই আমার request-এর reply
  }
}, { noAck: true });
ch.sendToQueue('rpc.payment', Buffer.from(JSON.stringify({ card, amount })),
  { correlationId, replyTo });

// ── Server (Payment Service): কাজ করে reply_to queue-তে একই correlationId সহ ফেরত দেয় ──
ch.consume('rpc.payment', (msg) => {
  const result = checkBalance(msg.content);
  ch.sendToQueue(msg.properties.replyTo, Buffer.from(result),
    { correlationId: msg.properties.correlationId });
  ch.ack(msg);
});
```

### Q6: Message Priority কীভাবে হ্যান্ডেল করবেন?
**সংক্ষিপ্ত উত্তর**: Queue declare করার সময় `x-max-priority` সেট করে priority queue বানানো যায়। মেসেজ পাঠানোর সময় `priority` ফিল্ড সেট করলে বেশি priority-র মেসেজ queue-তে জমে থাকা কম priority-র মেসেজের **আগে** process হয়।

নিচে গোড়া থেকে বিস্তারিত —

#### সমস্যাটা আগে বুঝি

সাধারণ Queue হলো **FIFO** (First In, First Out) — যে মেসেজ আগে ঢোকে সেটাই আগে বের হয়, ঠিক টিকিট কাউন্টারের লাইনের মতো। কিন্তু কিছু ক্ষেত্রে এটা যথেষ্ট না। ধরুন hospital-এর notification system:
- Queue-তে ১০০টা **normal** notification জমে আছে (যেমন "আপনার appointment কাল")
- হঠাৎ একটা **critical** alert এলো — "রোগীর heart rate বিপজ্জনক পর্যায়ে"

FIFO হলে critical alert-কে ওই ১০০টা normal মেসেজের **পেছনে** দাঁড়াতে হবে — যেটা মারাত্মক। আমরা চাই critical মেসেজ লাইন ভেঙে **সামনে** চলে যাক। এখানেই **Priority Queue**।

#### কীভাবে বানায় — ২ ধাপ

**ধাপ ১ — Queue declare করার সময় priority enable করা:**

```js
await ch.assertQueue('notifications', {
  durable: true,
  arguments: { 'x-max-priority': 10 }   // ০–১০ পর্যন্ত priority level
});
```

`x-max-priority: 10` মানে এই queue ০ থেকে ১০ পর্যন্ত priority বুঝবে। **এটা declare করার সময়েই সেট করতে হয়** — পরে বদলানো যায় না (তখন queue delete করে আবার বানাতে হয়)।

**ধাপ ২ — মেসেজ পাঠানোর সময় priority দেওয়া:**

```js
// critical alert — সবার আগে যাবে
ch.sendToQueue('notifications', Buffer.from(alertData),  { priority: 9 });

// normal notification — পরে
ch.sendToQueue('notifications', Buffer.from(normalData), { priority: 1 });
```

বেশি number = বেশি জরুরি = আগে deliver হবে। priority না দিলে default `0` ধরা হয়।

#### ⚠️ ৩টা গুরুত্বপূর্ণ ফাঁদ (এগুলোই ইন্টারভিউতে আলাদা করে)

**১. ইতিমধ্যে consumer-এর হাতে চলে যাওয়া মেসেজ আর reorder হয় না।**
Priority শুধু কাজ করে যেসব মেসেজ **queue-তে জমে আছে** তাদের মধ্যে। consumer fast থাকলে ও queue প্রায় খালি থাকলে priority-র কোনো effect-ই দেখা যায় না — জমে থাকার সুযোগই নেই। Priority তখনই কাজে লাগে যখন **backlog জমে** (producer speed > consumer speed)।

**২. Prefetch বেশি হলে priority নষ্ট হয়।**
consumer যদি `prefetch: 100` দিয়ে একসাথে ১০০টা মেসেজ নিজের কাছে টেনে নেয়, তাহলে সেই ১০০টার ভেতরে নতুন-আসা high-priority মেসেজ ঢুকতে পারে না — সেটা queue-তে অপেক্ষা করবে। তাই priority queue-তে **`prefetch: 1`** (বা খুব ছোট) রাখা ভালো, যাতে প্রতিবার consumer সবচেয়ে জরুরি মেসেজটাই তোলে।

**৩. Starvation সমস্যা।**
high-priority মেসেজ যদি অবিরাম আসতে থাকে, low-priority মেসেজ **কখনোই process হবে না** (অনাহারে থাকবে)। RabbitMQ এটা নিজে সমাধান করে না — design করার সময় মাথায় রাখতে হয় (আলাদা queue, বা priority level সীমিত রাখা)।

> **টিপস**: priority level বেশি (যেমন ২৫৫) রাখলে RabbitMQ প্রতি level-এর জন্য internal structure বানায় → memory/CPU খরচ বাড়ে। তাই সাধারণত **৫টার কম level** (যেমন low/normal/high = ১/৫/১০) রাখাই যথেষ্ট ও recommended।

#### Real world
Hospital-এর emergency notification system — critical alert (patient-এর vital sign খারাপ) সবসময় normal routine notification-এর আগে যাবে। (এটাই Real-World Projects সেকশনের [সমস্যা ১১](./10-Real-World-Projects-Part2.md#সমস্যা-১১--healthcare-system-ক্রিটিক্যাল-অ্যালার্ট-আগে) এর মূল ধারণা।)

#### এক লাইনে মূল কথা
> Priority queue মানে "লাইনে দাঁড়ানো মেসেজদের মধ্যে জরুরিটা আগে" — কিন্তু এটা কাজ করে **শুধু backlog থাকলে**, **prefetch ছোট রাখলে**, আর **starvation নিজে সামলাতে হবে**।

### Q7: একটা Queue-তে হঠাৎ মেসেজ জমে যাচ্ছে (backlog বাড়ছে) — কীভাবে handle করবেন?
**সংক্ষিপ্ত উত্তর**: consumer scale করা (horizontal scaling), consumer-এর কাজ optimize করা, queue length monitor করে auto-scale ও alert করা। নিচে বিস্তারিত —

#### Backlog মানে কী, আর কেন হয়

**Backlog** = queue-তে মেসেজ **ঢুকছে যত দ্রুত, বেরোচ্ছে তার চেয়ে ধীরে**। ফলে queue length বাড়তেই থাকে। মূল সমীকরণ সহজ:

> **producer speed > consumer speed** → backlog জমে।

কেন হতে পারে:
- **Traffic spike** — flash sale, black friday, viral post। হঠাৎ ১০x মেসেজ।
- **Consumer slow/down** — worker crash করেছে, বা প্রতিটা মেসেজ process করতে বেশি সময় লাগছে (slow database, slow third-party API)।
- **Poison message** — একটা মেসেজ বারবার fail করে requeue হয়ে লাইন আটকে রাখছে ([Q21](./13-Interview-QA-Q9-Q22.md#q21-poison-message-কী-এবং-কীভাবে-handle-করবেন) দেখুন)।

#### সমাধান — ধাপে ধাপে (আগে diagnose, পরে fix)

**১. Consumer বাড়ানো — Competing Consumers pattern (সবচেয়ে সরাসরি সমাধান)**
একই queue-তে একাধিক consumer bind করলে RabbitMQ round-robin করে কাজ ভাগ করে দেয়। ৪টা মেসেজ, ৪টা consumer → ৪ গুণ দ্রুত খালি হবে।

```js
ch.prefetch(10);                 // প্রতি consumer একসাথে ১০টা নিয়ে কাজ করবে (fair dispatch)
ch.consume('orders', handler);   // এই worker-টা আরও কয়েকটা instance-এ চালান
```
> এটাকে **horizontal scaling** বলে — worker instance সংখ্যা বাড়ানো।

**২. Consumer-এর নিজের কাজ দ্রুত করা (optimize)**
শুধু consumer বাড়ালেই হবে না, যদি bottleneck ভেতরে থাকে:
- প্রতিটা মেসেজে আলাদা DB query না করে **batch** করা
- Slow external API call async/parallel করা
- অপ্রয়োজনীয় ভারী কাজ queue-এর বাইরে সরানো

**৩. Auto-scaling — queue length দেখে dynamically worker বাড়ানো-কমানো**
Manual scaling যথেষ্ট না, কারণ spike অনিয়মিত। তাই queue length monitor করে অটোমেটিক scale করা হয়:
- Kubernetes **HPA** বা **KEDA** (KEDA সরাসরি RabbitMQ queue length দেখে pod বাড়ায়-কমায়)
- queue বড় হলে worker বাড়ে, খালি হলে আবার কমে যায় — খরচও বাঁচে।

**৪. Alerting — সমস্যা আগে টের পাওয়া**
queue length একটা threshold (যেমন ১০,০০০) ছাড়ালে যেন team জানতে পারে — Prometheus + Grafana, বা RabbitMQ management metrics দিয়ে। **আগে জানলে আগে সামলানো যায়।**

**৫. Backpressure / সুরক্ষা — queue যেন অসীম না বাড়ে**
consumer একেবারেই কুলিয়ে না উঠলে queue infinite বাড়তে থাকলে memory/disk alarm ট্রিগার হয়ে পুরো broker আটকে যেতে পারে ([Q17](./13-Interview-QA-Q9-Q22.md#q17-rabbitmq-তে-memory--disk-alarm-কী))। সুরক্ষা হিসেবে:
- **`x-max-length`** — queue-এর সর্বোচ্চ length বেঁধে দেওয়া; বেশি হলে পুরনো মেসেজ DLQ-তে যাবে।
- Producer-এর দিকে **rate limiting**।

#### এক লাইনে মূল কথা
> Backlog = consumer পিছিয়ে পড়ছে। সমাধান: **consumer বাড়াও (scale) + প্রতিটা consumer দ্রুত করো (optimize) + queue length monitor করে auto-scale ও alert করো।**

### Q8: Idempotency কেন দরকার এবং কীভাবে implement করবেন?
**সংক্ষিপ্ত উত্তর**: RabbitMQ "at-least-once delivery" guarantee দেয় — একই মেসেজ দুইবার (বা তার বেশি) deliver হতে পারে। তাই Consumer-এর কাজ **idempotent** হতে হবে — একই মেসেজ যতবারই আসুক, ফলাফল একই থাকবে। নিচে বিস্তারিত —

#### আগে বুঝি: RabbitMQ "at-least-once" দেয়, "exactly-once" নয়

RabbitMQ গ্যারান্টি দেয় মেসেজ **অন্তত একবার** deliver হবে — কিন্তু **একবারের বেশিও** হতে পারে। কেন duplicate হয়? ক্লাসিক scenario:

1. Consumer মেসেজ পেলো → কাজ সম্পূর্ণ করলো (যেমন কার্ড থেকে টাকা কাটলো)
2. `ack` পাঠানোর **ঠিক আগে** consumer crash করলো (বা network ছিঁড়ে গেলো)
3. RabbitMQ ack পায়নি → ধরে নিলো কাজ হয়নি → মেসেজটা **আবার** deliver করলো
4. আরেকটা consumer আবার টাকা কাটলো ❌ → **double charge**

এখানে RabbitMQ ভুল করেনি — এটাই at-least-once-এর স্বাভাবিক আচরণ। সমাধান broker নয়, **consumer-এর কোডে**।

#### Idempotency মানে কী

> একই মেসেজ **একবার বা একশোবার** process হলেও **ফলাফল একই** থাকবে।

গণিতে যেমন `×1` — যতবার গুণ করো, মান বদলায় না। target হলো: duplicate মেসেজ এলে সিস্টেমের state যেন না বদলায়।

#### কীভাবে implement করবেন

**মূল আইডিয়া**: প্রতিটা মেসেজের সাথে একটা **unique ID** (transaction ID / message ID / idempotency key) পাঠান, আর process করার আগে check করুন — এই ID কি আগে দেখেছি?

```js
ch.consume('payments', async (msg) => {
  const { txnId, userId, amount } = JSON.parse(msg.content.toString());

  // ১. আগে process হয়েছে কিনা check
  const alreadyDone = await db.exists('processed_txns', txnId);
  if (alreadyDone) {
    ch.ack(msg);          // duplicate — কিছু না করেই ack, মেসেজ ফেলে দাও
    return;
  }

  // ২. আসল কাজ + ID রেকর্ড — একই DB transaction-এ (atomic)
  await db.transaction(async (t) => {
    await chargeCard(userId, amount, t);
    await t.insert('processed_txns', { txnId });   // "দেখেছি" চিহ্ন
  });

  ch.ack(msg);
});
```

**গুরুত্বপূর্ণ সূক্ষ্মতা** — কাজ করা আর ID রেকর্ড করা **একই atomic transaction-এ** হতে হবে। নাহলে: টাকা কাটার পর, ID লেখার আগে crash করলে → আবার duplicate। দুটো একসাথে commit হলে এই ফাঁক থাকে না।

#### Idempotency-র বিভিন্ন কৌশল

| কৌশল | কীভাবে | কখন |
|---|---|---|
| **Dedup table** | processed ID একটা টেবিলে রাখা, insert-এর আগে check | সবচেয়ে common, general purpose |
| **Unique DB constraint** | `txnId`-কে unique key বানানো; duplicate insert নিজে থেকেই fail করবে | DB-native, সহজ |
| **Upsert / idempotent operation** | operation নিজেই idempotent — যেমন `SET status = 'paid'` (দুইবার করলেও একই) | state overwrite হলে চলে |
| **Redis SETNX + TTL** | দ্রুত in-memory dedup check | high throughput, short window |

#### Real world
Payment processing-এ — একই "charge_user" মেসেজ দুইবার এলে যেন ইউজারের কার্ড থেকে দুইবার টাকা না কাটে। সমাধান: প্রতিটা মেসেজের সাথে একটা unique transaction ID পাঠানো, এবং process করার আগে check করা এই ID আগে process হয়েছে কিনা (database-এ record রেখে)। (Real-World Projects সেকশনের [সমস্যা ২](./09-Real-World-Projects-Part1.md#সমস্যা-২-payment-webhook-দুইবার-এসে-দুইবার-টাকা-কাটছে-idempotency) এরই বিস্তারিত রূপ।)

#### এক লাইনে মূল কথা
> RabbitMQ at-least-once দেয় বলে duplicate অনিবার্য — তাই consumer-কে **idempotent** বানাও: প্রতিটা মেসেজে **unique ID**, আর কাজ + "দেখেছি" রেকর্ড **একই atomic transaction-এ**।

---

---

[⬅ 11-Interview-Questions-All-Levels.md](./11-Interview-Questions-All-Levels.md) | [13-Interview-QA-Q9-Q22.md ➡](./13-Interview-QA-Q9-Q22.md) | [🏠 Repo Home](../README.md)
