# Real-World Projects (১–৯) — সমস্যা ও সমাধান কোডসহ


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

#### 🔍 আরও গভীরে — ভেতরে কী ঘটছে

- **Synchronous-এ latency কেন জমে**: sign-up handler একের পর এক email → SMS → CRM call করে; প্রতিটা external call-এর latency (100ms + 200ms + 2s…) **যোগ** হয়ে API response ৩–৫s। এর যেকোনো একটা fail করলে পুরো request 500 error — ইউজার sign-up-ই করতে পারে না, যদিও আসল কাজ (DB save) হয়ে গেছে।
- **Decouple করলে কী হয়**: শুধু একটা event `publish` করে সাথে সাথে response — API latency এখন কেবল "broker-এ লেখা" (~ms)। বাকি সব side-effect background consumer করে।
- **কোডের প্রতিটা অংশ কেন জরুরি**:
  - `createConfirmChannel()` + `await waitForConfirms()` → **publisher confirm**। broker সত্যিই মেসেজ পেয়েছে নিশ্চিত না হয়ে "success" ফেরত দিলে, মেসেজ যদি হারায় ইউজার কখনো welcome email পাবে না। confirm এই ফাঁক বন্ধ করে।
  - `persistent: true` + durable exchange/queue → broker restart হলেও মেসেজ বাঁচে।
  - `messageId: signup-<id>` → consumer-এ dedup key; redelivery হলে duplicate email ঠেকাতে (Q8-এর idempotency)।
  - consumer-এ `nack(msg, false, false)` → fail হলে requeue **নয়**, সরাসরি DLQ; নাহলে একই bad মেসেজ অনন্তকাল retry হয়ে queue আটকাবে।
- **Trade-off**: এখন **eventual consistency** — ইউজার "success" দেখলেও welcome email হয়তো ১–২s পরে যায়। email/SMS/analytics-এর মতো side-effect-এ এটা সম্পূর্ণ গ্রহণযোগ্য।
- **মূল শিক্ষা**: "ইউজারকে যা সাথে সাথে জানাতে হবে" (DB save) সেটুকু sync রাখো; বাকি সব async event-এ ফেলো।

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

#### 🔍 আরও গভীরে — কেন duplicate অনিবার্য, আর কীভাবে ঠেকে

- **Duplicate কেন আসে (দুই কারণ)**: (১) RabbitMQ **at-least-once** — consumer টাকা কেটে ফেলেও `ack` পাঠানোর ঠিক আগে crash/network drop করলে broker ধরে নেয় "হয়নি", আবার deliver করে। (২) Payment provider নিজেই একই webhook দুইবার পাঠাতে পারে (তাদের retry)।
- **মূল চাবি — unique key**: প্রতিটা payment attempt-এর একটা `transaction_id`। এটাই দিয়ে ঠিক করা হয় "এই কাজ কি আগে করেছি?"
- **সবচেয়ে সূক্ষ্ম জায়গা — atomicity**: `charge_card()` আর `db.mark_processed(txn_id)` অবশ্যই **একই DB transaction-এ** commit হতে হবে। নাহলে টাকা কাটার পর, mark করার আগে crash করলে → পরের delivery-তে আবার কাটবে। দুটো একসাথে commit হলে এই ফাঁক থাকে না।
- **`already_processed` চেকটা race-safe হতে হবে**: শুধু `SELECT` করে "নেই" দেখে insert করলে — দুই consumer একসাথে একই txn চেক করলে দুজনেই "নেই" দেখে দুবার charge করতে পারে। নিরাপদ উপায়: `txn_id`-কে **unique constraint** বানিয়ে সরাসরি insert চেষ্টা করা; duplicate হলে DB নিজেই আটকাবে (দেখুন Q8-এর কৌশল টেবিল)।
- **Transient vs Permanent error আলাদা করা**: `TransientError` (নেটওয়ার্ক টাইমআউট) → `requeue=True`, পরে আবার হবে। `PermanentError` (কার্ড invalid/expired) → `requeue=False` → DLQ, কারণ retry করে লাভ নেই, শুধু queue আটকাবে।
- **মূল শিক্ষা**: broker exactly-once দিতে পারে না — নিরাপত্তা তোমার consumer-এর কোডে (unique id + atomic dedup)।

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

#### 🔍 আরও গভীরে — order কীভাবে রক্ষা পায়

- **কেন উল্টে যায়**: এক queue-তে দুই consumer round-robin ভাগ করে নেয়। C1 fast, C2 slow হলে `withdraw` (C2-তে) `deposit` (C1-তে)-এর আগেই শেষ হয়ে balance ভুল করে। queue-এর order ঠিক ছিল, কিন্তু **processing order** ভাঙল।
- **Consistent-Hash Exchange কীভাবে ঠিক করে**: routing key = `accountId`। exchange সেই key **hash** করে সবসময় একই shard queue বেছে নেয় (deterministic, random নয়)। ফলে একই account-এর *সব* মেসেজ একই queue → একই consumer → FIFO রক্ষা।
- **`bindQueue(q, 'txn', '1')`-এর `'1'` কী**: এটা shard-এর **weight** (hash-ring-এ ওজন)। সব shard-এ সমান weight দিলে account গুলো shard-গুলোতে সমানভাবে ছড়ায়।
- **Scaling রক্ষা পায় যেভাবে**: ১০টা shard = ১০টা consumer parallel-এ কাজ করছে। শুধু **per-account** order দরকার, **global** order নয় — তাই ভিন্ন account ভিন্ন shard-এ যাওয়ায় কোনো সমস্যা নেই, বরং throughput ১০ গুণ।
- **⚠️ Gotcha**: একটা shard queue-তে **একাধিক** consumer bসালালে আবার order ভাঙবে। তাই shard-প্রতি **একটাই** consumer (বা `x-single-active-consumer` দিয়ে auto-failover সহ একজন active)।
- **মূল শিক্ষা**: "একই entity = একই queue = একই consumer" — এই এক নীতিতেই বাস্তবের ৯০% ordering সমস্যা সমাধান হয়।

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

#### 🔍 আরও গভীরে — Work Queue ও prefetch দিয়ে গতি নিয়ন্ত্রণ

- **দুইটা আলাদা সমস্যা**: (১) মূল অ্যাপ ১০ লক্ষ ইমেইল পাঠাতে গিয়ে block হয়ে যায়; (২) email provider প্রতি সেকেন্ডে সীমিত মেইল নেয় (rate-limit) — বেশি পাঠালে block/bounce।
- **Work Queue (Competing Consumers)**: এক `email.bulk` queue, বহু worker। RabbitMQ round-robin করে কাজ ভাগ করে — প্রতিটা মেসেজ **একজনই** পায় (broadcast নয়)।
- **`prefetch(50)` কী নিয়ন্ত্রণ করে**: প্রতি worker একসাথে সর্বোচ্চ ৫০টা মেসেজ ধরে। এটাই কার্যত throttle — provider যত সহ্য করে সেই অনুযায়ী tune করো। খুব বেশি হলে rate-limit hit, খুব কম হলে worker idle বসে (দেখুন Q32 prefetch tuning)।
- **Scale করা মানে শুধু worker বাড়ানো**: throughput বাড়াতে আরও worker pod চালাও — **অ্যাপ কোড বা producer অপরিবর্তিত**। এটাই queue-এর সৌন্দর্য: producer আর consumer আলাদাভাবে scale করে।
- **`x-max-length` কেন**: backlog-এর সীমা; মেসেজ অসীম জমে গেলে broker-এর memory/disk alarm উঠতে পারে (Q17)। সীমা ছাড়ালে overflow policy অনুযায়ী drop/DLQ (Q26)।
- **⚠️ Gotcha**: prefetch একদম নিখুঁত rate-limit দেয় না; কড়া rate control দরকার হলে worker-এ একটা token-bucket/throttle যোগ করতে হয়।
- **মূল শিক্ষা**: producer শুধু জব ঢালে, worker pool "নিজের হজমক্ষমতা" অনুযায়ী খায় — prefetch সেই হজমক্ষমতার নিয়ন্ত্রক।

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

#### 🔍 আরও গভীরে — ভারী job-এ prefetch=1 কেন

- **কেন async করতেই হবে**: transcode-এ কয়েক মিনিট লাগে; HTTP request thread-এ করলে timeout, আর ইউজার ততক্ষণ আটকে। তাই upload-এর সাথে সাথে শুধু একটা "transcode job" queue-তে ফেলে "processing…" দেখানো হয়।
- **`prefetch=1` এখানে কেন গুরুত্বপূর্ণ**: প্রতিটা job অত্যন্ত ভারী (CPU/GPU-বাউন্ড, মিনিট-লম্বা)। prefetch বেশি হলে একটা worker অনেকগুলো job নিজের কাছে টেনে নেবে আর সেগুলো লাইনে বসে থাকবে যখন অন্য worker idle। `prefetch=1` মানে "একটা শেষ করে তবেই পরেরটা নাও" — fair distribution।
- **`priority: 5` ইঙ্গিত**: চাইলে premium ইউজারের ভিডিও আগে (priority queue, Q6)।
- **`delivery_mode=2` (persistent) কেন জরুরি**: transcode-এর মাঝপথে GPU worker crash করলে job যেন হারিয়ে না যায় — ack না হওয়া পর্যন্ত broker ধরে রাখে, redeliver করে।
- **Completion feedback**: worker শেষ করে আরেকটা event (`video.ready`) ছাড়ে → status "done"-এ আপডেট হয় (আরেকটা queue বা WebSocket দিয়ে ইউজারকে জানানো)।
- **মূল শিক্ষা**: ভারী, দীর্ঘ job = আলাদা worker + `prefetch=1` + persistent — যাতে একটা worker আটকালে বা crash করলেও কাজ নিরাপদ ও সমানভাবে ভাগ থাকে।

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

#### 🔍 আরও গভীরে — TTL + DLX দিয়ে delayed retry কীভাবে হয়

- **কেন সরাসরি requeue খারাপ**: fail হওয়ামাত্র `nack(requeue=true)` করলে মেসেজ সাথে সাথে আবার সামনে আসে → busy-loop, provider-এর উপর আরও চাপ, rate-limit আরও খারাপ হয়।
- **Delay queue-এর ট্রিক**: `sms.retry.30s` queue-তে **কোনো consumer নেই**, কিন্তু `x-message-ttl: 30000`। মেসেজ এখানে ৩০ সেকেন্ড চুপচাপ বসে থাকে, TTL শেষ হলে `x-dead-letter-routing-key` দিয়ে **আবার মূল `sms.send` queue-তে** ফিরে যায়। ফলাফল: ৩০ সেকেন্ড পরে retry — কোনো `sleep` কোড ছাড়াই, broker-ই delay সামলায়।
- **Retry count কীভাবে বাড়ে**: মেসেজের `x-retry` header প্রতিবার +১; ৫ ছাড়ালে `nack(false,false)` → DLQ + alert (মানুষ দেখবে)।
- **⚠️ সবচেয়ে সহজে ভুল হওয়া জায়গা**: retry queue-তে নতুন কপি পাঠানোর পর **পুরনো মেসেজটা `ack` করে সরাতে হবে** (কোডে `ch.ack(msg)`), নাহলে একই মেসেজ দুই জায়গায় — duplicate।
- **Exponential backoff**: 30s → 60s → 120s চাইলে আলাদা আলাদা TTL-এর retry queue (`retry.30s`, `retry.60s`…), অথবা `rabbitmq-delayed-message-exchange` plugin দিয়ে per-message `x-delay` (Q12)।
- **মূল শিক্ষা**: "অপেক্ষা করে আবার চেষ্টা" — এটা consumer-এ `sleep` দিয়ে নয়, **TTL+DLX** দিয়ে করাই RabbitMQ-native ও non-blocking।

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

#### 🔍 আরও গভীরে — `reply_to` + `correlation_id` কীভাবে sync-এর ভান করে

- **সমস্যাটা**: RabbitMQ স্বভাবতই fire-and-forget (পাঠিয়ে ভুলে যাও)। কিন্তু Order Service-কে Payment Service-এর "টাকা আছে কি নেই" **উত্তরটা** লাগবে তবেই এগোবে।
- **কীভাবে কাজ করে (৪ ধাপ)**:
  1. Client একটা **temporary reply queue** বানায় (`exclusive: true` — শুধু তার, connection বন্ধ হলে মুছে যায়)।
  2. request পাঠায় দুটো property সহ: `reply_to` (উত্তর কোথায় দেবে) + `correlation_id` (একটা unique UUID)।
  3. Server কাজ করে ঐ `reply_to` queue-তে result পাঠায়, **একই `correlation_id`** বসিয়ে।
  4. Client reply queue শোনে; `correlation_id` মিলিয়ে বোঝে "এটাই আমার ঐ request-এর উত্তর"।
- **`correlation_id` কেন লাগে**: একটা client একসাথে বহু request পাঠাতে পারে, সব উত্তর একই reply queue-তে আসে — কোন উত্তর কোন request-এর, সেটা মেলাতে এই id।
- **⚠️ Timeout অপরিহার্য**: server crash করে উত্তর না দিলে client যেন চিরকাল না ঝোলে — একটা timeout রেখে fail/retry করতে হয়।
- **⚠️ Anti-pattern সতর্কতা**: RPC মানে আবার coupling + blocking ফিরিয়ে আনা। সত্যিই যদি synchronous উত্তরই দরকার, অনেক সময় সরাসরি **HTTP/gRPC** সহজ। RabbitMQ RPC তখনই যুক্তিযুক্ত যখন broker-এর load-balancing/routing/back-pressure সুবিধাও চাই।
- **মূল শিক্ষা**: RPC হলো async transport-এর উপর sync request-response সাজানো — `reply_to` + `correlation_id` এই দুটোই মূল।

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

#### 🔍 আরও গভীরে — Fanout দিয়ে service-গুলো কীভাবে স্বাধীন থাকে

- **মূল চাওয়া**: order placed হলে Inventory, Invoice, Notification, Analytics — সবাই জানবে, কিন্তু **কেউ কারো উপর নির্ভর করবে না** আর producer-কে জানতেই হবে না কে কে শুনছে।
- **Fanout কীভাবে কাজ করে**: fanout exchange **routing key উপেক্ষা** করে, তার সাথে bound *প্রতিটা* queue-তে মেসেজের একটা কপি পাঠায়। order publish হয় **একবার**, চারটা service চারটা কপি পায়।
- **প্রতিটা service-এর নিজস্ব durable queue কেন**: Analytics service ১০ মিনিট down থাকলেও তার queue-তে মেসেজ জমতে থাকে; ফিরে এসে process করে। এদিকে Inventory/Invoice নির্বিঘ্নে চলে — একজনের ব্যর্থতা অন্যকে স্পর্শ করে না। (একটা শেয়ার্ড queue হলে এটা সম্ভব হতো না।)
- **Fanout vs Topic**: সবাই সব event চাইলে fanout। কিছু service শুধু কিছু event চাইলে (যেমন শুধু `order.cancelled`) **topic exchange** + pattern binding।
- **⚠️ Trade-off**: এক event-এর N কপি মানে RabbitMQ-তে **storage duplicate**। খুব বিশাল volume + বহু consumer group হলে Kafka/Stream বেশি efficient (সেকশন ৮ ও Q24)।
- **মূল শিক্ষা**: "একজন নেবে" = শেয়ার্ড queue (competing consumers); "সবাই নেবে" = fanout, প্রত্যেকের আলাদা queue (Q19)।

### সমস্যা ৯ — [Ride-Sharing App] ড্রাইভার-রাইডার ম্যাচিং ও লাইভ লোকেশন

**Project type**: Ride-Hailing Platform (Uber / Pathao / inDrive টাইপ)

**সমস্যা**: রাইড রিকোয়েস্ট এলে আশেপাশের driver খুঁজে notification পাঠাতে হয়, আবার প্রতি সেকেন্ডে হাজার হাজার driver-এর GPS location আপডেট আসে — সব sync করলে API চাপে ভেঙে পড়ে।

**সমাধান**: রাইড ইভেন্টের জন্য **topic exchange** (city/zone অনুযায়ী routing), আর location update-এর জন্য আলাদা high-throughput queue — matching service background-এ কাজ করে।

```js
await ch.assertExchange('ride', 'topic', { durable: true });

// zone অনুযায়ী শুধু ঐ এলাকার driver-notification service শোনে
await ch.assertQueue('match.dhaka.uttara', { durable: true });
await ch.bindQueue('match.dhaka.uttara', 'ride', 'ride.requested.dhaka.uttara');

// rider request → শুধু সংশ্লিষ্ট zone-এ যায় (whole system-এ broadcast নয়)
ch.publish('ride', `ride.requested.dhaka.uttara`,
  Buffer.from(JSON.stringify({ riderId, pickup })), { persistent: true });
```

#### 🔍 আরও গভীরে — Topic routing দিয়ে "শুধু দরকারিরা" শোনে

- **দুই ধরনের সম্পূর্ণ ভিন্ন load**: (১) ride request — সংখ্যায় কম, কিন্তু সঠিক zone-এ পৌঁছানো জরুরি; (২) GPS location update — প্রতি সেকেন্ডে হাজার হাজার, বিশাল volume কিন্তু প্রতিটা কম গুরুত্বপূর্ণ।
- **Topic exchange কেন**: routing key `ride.requested.dhaka.uttara`-তে binding `ride.requested.dhaka.uttara` (বা `ride.requested.dhaka.*`) দিলে **শুধু ঐ zone-এর** matching service মেসেজ পায় — পুরো সিস্টেমে broadcast করে সবার CPU নষ্ট করতে হয় না। wildcard (`*` = এক শব্দ, `#` = একাধিক) দিয়ে "ঢাকার সব zone" ইত্যাদি নমনীয় routing।
- **Location update আলাদা কেন**: এটা high-throughput, ephemeral। এখানে সাধারণত **persistent করা হয় না** (নতুন location পুরনোটাকে অপ্রাসঙ্গিক করে দেয়) — কিছু update drop হলেও ক্ষতি নেই, বরং throughput আগে। চাইলে lazy queue বা Stream।
- **⚠️ Gotcha**: ride request-এর মতো গুরুত্বপূর্ণ মেসেজ persistent + confirm; location-এর মতো "সর্বশেষটাই আসল" ডেটাকে persistent করলে অযথা disk I/O বাড়ে।
- **মূল শিক্ষা**: এক সিস্টেমে সব মেসেজ সমান নয় — critical (ride) আর firehose (location) কে **আলাদা exchange/queue + আলাদা reliability সেটিং** দাও।

---

[⬅ 08-RabbitMQ-vs-Kafka.md](./08-RabbitMQ-vs-Kafka.md) | [10-Real-World-Projects-Part2.md ➡](./10-Real-World-Projects-Part2.md) | [🏠 Repo Home](../README.md)
