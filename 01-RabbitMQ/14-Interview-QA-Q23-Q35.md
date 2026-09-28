# আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q23–Q35)


এই প্রশ্নগুলো protocol, নতুন queue type (Stream), cluster-level edge case, security আর operational দিক নিয়ে — senior/architect লেভেলের ইন্টারভিউতে এগুলোই আলাদা করে দেয়।

### Q23: AMQP protocol কী এবং এর মূল অংশগুলো কী?
**উত্তর**: RabbitMQ মূলত **AMQP 0-9-1** (Advanced Message Queuing Protocol) implement করে — এটা একটা open, binary, application-layer messaging protocol। HTTP যেমন web-এর জন্য standard, AMQP তেমনি messaging-এর জন্য একটা standard, তাই যেকোনো ভাষার client একই protocol দিয়ে RabbitMQ-এর সাথে কথা বলতে পারে।

**AMQP-এর মূল building block:**
- **Connection** — একটা TCP connection (costly)
- **Channel** — connection-এর ভেতরে virtual, হালকা "sub-connection"
- **Exchange** — মেসেজ কোথায় যাবে ঠিক করে (routing)
- **Queue** — মেসেজ জমা থাকে
- **Binding** — exchange আর queue-এর মধ্যে rule (routing key/pattern)
- **Message** — দুই অংশ: **properties** (headers, delivery_mode, priority, correlation_id…) + **body/payload**

**গুরুত্বপূর্ণ**: RabbitMQ শুধু AMQP-তে সীমাবদ্ধ নয় — plugin দিয়ে **MQTT** (IoT/mobile), **STOMP** (simple text), **AMQP 1.0**, আর **WebSocket** protocol-ও সাপোর্ট করে। তাই একটা device MQTT দিয়ে publish করে, backend AMQP দিয়ে consume করতে পারে — একই broker।

> **এক লাইনে**: AMQP 0-9-1 হলো RabbitMQ-এর "মাতৃভাষা" (connection → channel → exchange → binding → queue), আর MQTT/STOMP হলো plugin দিয়ে যোগ করা অতিরিক্ত ভাষা।

### Q24: RabbitMQ Streams (Stream Queue) কী — Kafka-এর মতো replay কি এখন সম্ভব?
**উত্তর**: হ্যাঁ। **Stream** হলো RabbitMQ 3.9-এ আসা একটা নতুন queue type, যেটা Kafka-এর মতো **append-only, non-destructive log**। সাধারণ queue-তে consume করলে মেসেজ মুছে যায়; Stream-এ মেসেজ **মোছে না** — retention (সময়/সাইজ) অনুযায়ী থাকে, আর consumer একটা **offset** থেকে পড়ে।

**এটা কীভাবে Kafka-এর কাছাকাছি:**
- একই ডেটা **অনেক consumer** স্বাধীনভাবে পড়তে পারে (offset নিজে ট্র্যাক করে)
- চাইলে offset পিছিয়ে দিয়ে **পুরনো ডেটা replay** করা যায়
- বিশাল throughput — একটা dedicated binary **stream protocol** আছে (তবে AMQP দিয়েও পড়া যায়)
- disk-এ থাকে, তাই লক্ষ-কোটি মেসেজ রাখা যায়

```js
// Stream queue — replay + বহু consumer একই ডেটা পড়তে পারে
await ch.assertQueue('events.stream', {
  durable: true,
  arguments: {
    'x-queue-type': 'stream',
    'x-max-length-bytes': 5_000_000_000,   // retention (সাইজ দিয়ে)
    'x-stream-max-age': '7D'               // অথবা সময় দিয়ে (৭ দিন)
  }
});
// consumer offset দিয়ে পড়ে — first/last/next/নির্দিষ্ট offset/timestamp থেকে
await ch.consume('events.stream', handler, {
  arguments: { 'x-stream-offset': 'first' }   // একদম শুরু থেকে replay
});
```

**কখন Stream, কখন সাধারণ Queue**: ইভেন্ট history/replay/একই ডেটা multiple team → **Stream**। এক-মেসেজ-এক-consumer task processing (payment, order) → সাধারণ **Quorum/Classic Queue**।

> **এক লাইনে**: Stream দিয়ে RabbitMQ এখন Kafka-এর মূল সুবিধা (replay + non-destructive multi-consumer read) দিতে পারে — কিন্তু task-queue কাজের জন্য এখনও সাধারণ queue-ই ঠিক।

### Q25: Lazy Queue কী এবং কখন ব্যবহার করবেন?
**উত্তর**: সাধারণ classic queue যতটা পারে মেসেজ **RAM-এ** ধরে রাখে (দ্রুত delivery-র জন্য)। কিন্তু queue যদি বিশাল হয় (লক্ষ লক্ষ মেসেজ backlog), RAM-এ চাপ পড়ে memory alarm ট্রিগার হতে পারে। **Lazy Queue** যত তাড়াতাড়ি সম্ভব মেসেজ **disk-এ লিখে ফেলে**, RAM-এ কম রাখে — throughput সামান্য কমে, কিন্তু বিশাল backlog-এ broker স্থিতিশীল থাকে।

**কখন দরকার**: batch/IoT ingestion, খুব বড় backlog সম্ভব এমন queue, বা consumer মাঝে মাঝে অনেকক্ষণ down থাকে।

```python
channel.queue_declare('sensor.readings', durable=True,
    arguments={'x-queue-mode': 'lazy'})   # RAM নয়, disk-এ backlog রাখে
```

> **নোট (আধুনিক ভার্সন)**: RabbitMQ 3.12+ থেকে classic queue **v2**-এ lazy behavior মূলত default হয়ে গেছে, তাই `x-queue-mode: lazy` এখন অনেকটা deprecated/অপ্রয়োজনীয়। ইন্টারভিউতে concept জানা জরুরি, কিন্তু নতুন version-এ আলাদা করে সেট করার দরকার কমে গেছে — এটা বললে ভালো ইম্প্রেশন পড়ে।

### Q26: Queue ভরে গেলে (overflow) কী হয়? drop-head বনাম reject-publish
**উত্তর**: `x-max-length` (মেসেজ সংখ্যা) বা `x-max-length-bytes` (সাইজ) দিয়ে queue-এর সীমা বাঁধা যায়। সীমা ছাড়ালে কী হবে তা `x-overflow` ঠিক করে:
- **`drop-head`** (default): সবচেয়ে **পুরনো** মেসেজ (queue-এর head) ফেলে দেয়, নতুন মেসেজ ঢোকে।
- **`reject-publish`**: নতুন publish **reject** হয় — publisher confirm ব্যবহার করলে producer একটা `nack` পায় (backpressure), তাই producer বুঝতে পারে queue ভরা।
- **`reject-publish-dlx`**: reject হওয়া মেসেজ DLX-এ পাঠায় (হারায় না)।

```js
await ch.assertQueue('orders', {
  durable: true,
  arguments: {
    'x-max-length': 100000,
    'x-overflow': 'reject-publish',              // পুরনো মেসেজ না ফেলে নতুনটা reject
    'x-dead-letter-exchange': 'orders.dlx'       // চাইলে reject-publish-dlx দিয়ে DLQ-তে
  }
});
```

**কোনটা কখন**: পুরনো ডেটা মূল্যহীন হলে (যেমন live metric) → `drop-head`। প্রতিটা মেসেজ গুরুত্বপূর্ণ (order/payment) → `reject-publish`/`reject-publish-dlx`, যাতে চুপচাপ ডেটা না হারায়।

> **এক লাইনে**: overflow মানে "queue ভরা" — `drop-head` পুরনোটা ফেলে, `reject-publish` নতুনটা আটকে producer-কে backpressure দেয়।

### Q27: Alternate Exchange (AE) কী — unroutable মেসেজ কোথায় যায়?
**উত্তর**: একটা মেসেজ যদি exchange-এ আসে কিন্তু কোনো binding-এর সাথে ম্যাচ না করে (কোনো queue-তে route হয় না), তখন সেটা **নিঃশব্দে হারিয়ে যায়** (যদি না `mandatory` flag দিয়ে producer-কে ফেরত পাঠানো হয়)। **Alternate Exchange** হলো একটা "fallback" exchange — এমন unroutable মেসেজ drop না হয়ে সেখানে চলে যায়, যাতে ধরা যায় "কোন মেসেজ কোথাও পৌঁছায়নি"।

```js
// AE নিজে একটা exchange — এখানে unroutable মেসেজ জমা হয়
await ch.assertExchange('orders.unrouted', 'fanout', { durable: true });
await ch.assertQueue('unrouted.inbox', { durable: true });
await ch.bindQueue('unrouted.inbox', 'orders.unrouted', '');

// মূল exchange-এ alternate-exchange সেট করা
await ch.assertExchange('orders', 'direct', {
  durable: true,
  arguments: { 'alternate-exchange': 'orders.unrouted' }
});
// ভুল routing key-র মেসেজ drop না হয়ে unrouted.inbox-এ যাবে → পরে debug করা যায়
```

**Use case**: routing bug ধরা, misconfigured producer সনাক্ত করা, "catch-all" logging।

> **এক লাইনে**: AE = unroutable মেসেজের নিরাপত্তা জাল — কোনো queue না পেলে drop না হয়ে fallback exchange-এ যায়।

### Q28: Cluster-এ Network Partition (split-brain) হলে কী হয়?
**উত্তর**: Cluster-এর নোডগুলোর মধ্যে network link ছিঁড়ে গেলে দুই (বা তার বেশি) দল আলাদা হয়ে যায় — প্রতিটা দল ভাবতে পারে সে-ই আসল ("split-brain")। classic mirrored queue-তে এটা মারাত্মক ছিল (Q7/সেকশন ৭ দ্রষ্টব্য)। RabbitMQ-তে `cluster_partition_handling` দিয়ে strategy ঠিক করা হয়:
- **`ignore`**: কিছু করে না — ছোট cluster/ম্যানুয়াল হ্যান্ডলিং, ঝুঁকিপূর্ণ।
- **`pause_minority`** (recommended): যে দল **minority** (অর্ধেকের কম নোড), তারা নিজেদের **pause** করে দেয় — শুধু majority দল কাজ করে, তাই consistency বজায় থাকে।
- **`autoheal`**: partition মিটলে একটা "winner" দল ঠিক করে বাকিদের restart করে merge করে — availability-কে বেশি গুরুত্ব দেয়।

```ini
# rabbitmq.conf — consistency-focused (সবচেয়ে বেশি ব্যবহৃত)
cluster_partition_handling = pause_minority
```

**আধুনিক সমাধান**: **Quorum Queue** ও **Stream** যেহেতু Raft-ভিত্তিক, তারা majority-quorum দিয়ে নিজেরাই split-brain সামলায় (minority দিকে write হয় না)। তাই HA queue হিসেবে Quorum ব্যবহার করলে এই সমস্যাটা অনেকখানি মিটে যায়।

> **এক লাইনে**: split-brain এড়াতে `pause_minority` + Quorum Queue — majority ছাড়া কেউ write করবে না, তাই দুই "master" তৈরি হবে না।

### Q29: durable, persistent, transient, exclusive, auto-delete — পার্থক্য কী?
**উত্তর**: এগুলো প্রায়ই গুলিয়ে যায়, কিন্তু আলাদা জিনিস:

| শব্দ | কার প্রপার্টি | মানে |
|---|---|---|
| **durable** | Queue/Exchange | broker restart হলেও **definition** টিকে থাকে (queue-টা থেকে যায়) |
| **persistent** | Message (`delivery_mode: 2`) | মেসেজ **disk-এ** লেখা হয়, শুধু RAM-এ না |
| **transient** | Message (`delivery_mode: 1`) | মেসেজ শুধু RAM-এ, restart হলে হারায় |
| **exclusive** | Queue | শুধু যে connection বানিয়েছে সে-ই ব্যবহার করে; connection বন্ধ হলে queue **auto-delete** |
| **auto-delete** | Queue/Exchange | শেষ consumer/binding চলে গেলে নিজে থেকে মুছে যায় |

**সবচেয়ে গুরুত্বপূর্ণ ভুল ধারণা**: শুধু `durable queue` করলেই মেসেজ টেকে না, আবার শুধু `persistent message` করলেও না — মেসেজ restart-এ টিকতে হলে **queue durable + message persistent দুটোই** লাগবে (আর নিশ্চিত delivery-র জন্য publisher confirm)।

```js
await ch.assertQueue('orders', { durable: true });                 // definition টেকে
ch.sendToQueue('orders', Buffer.from(data), { persistent: true }); // মেসেজও disk-এ
// দুটো একসাথে হলে তবেই broker restart-এ মেসেজ থাকবে
```

> **এক লাইনে**: durable = queue টেকে, persistent = মেসেজ টেকে, exclusive/auto-delete = queue নিজে নিজে মুছে যায় — restart-safe হতে **durable + persistent** দুটোই চাই।

### Q30: RabbitMQ-তে security কীভাবে হ্যান্ডেল করবেন?
**উত্তর**: তিন স্তরে ভাবুন — **কে ঢুকবে (authentication), কে কী করতে পারবে (authorization), তার (encryption)।**

1. **Authentication (কে)**: username/password (default), বা আরও শক্ত — **x.509 client certificate**, **LDAP**, **OAuth 2.0/JWT** (plugin দিয়ে)।
2. **Authorization (কী করতে পারবে)**: প্রতিটা user-কে প্রতিটা **vhost**-এ তিনটা regex permission দেওয়া হয় — **configure** (queue/exchange বানানো), **write** (publish), **read** (consume)।
3. **Encryption (গোপনীয়তা)**: **TLS** দিয়ে client↔broker আর node↔node ট্রাফিক encrypt করা।

```bash
# user বানানো ও নির্দিষ্ট permission — least privilege
rabbitmqctl add_user app_payments 'S3cret!'
rabbitmqctl set_permissions -p payments app_payments \
   "^payments\."   "^payments\."   "^payments\."     # configure / write / read regex
#   → এই user শুধু "payments." দিয়ে শুরু হওয়া resource ব্যবহার করতে পারবে
```

**Production best practice**:
- default **`guest`** user শুধু localhost থেকে কাজ করে — prod-এ কখনো guest ব্যবহার করবেন না, নতুন user বানান।
- প্রতিটা service-কে **আলাদা user + minimal permission** দিন (least privilege)।
- সবসময় **TLS** চালু রাখুন, আর management UI public internet-এ খুলে রাখবেন না।

> **এক লাইনে**: security = authentication (কে) + per-vhost regex authorization (কী) + TLS (encryption) — আর prod-এ guest নয়, least-privilege user।

### Q31: at-most-once, at-least-once, exactly-once — delivery guarantee গুলো কী?
**উত্তর**: এটা messaging-এর সবচেয়ে মৌলিক (ও tricky) concept:

| Guarantee | কীভাবে হয় | ট্রেড-অফ |
|---|---|---|
| **At-most-once** | auto-ack / non-persistent — একবার পাঠায়, হারালে হারালো | **duplicate নেই**, কিন্তু মেসেজ **হারাতে পারে** |
| **At-least-once** | manual ack + persistent + confirm — ack না পেলে redeliver | **হারায় না**, কিন্তু **duplicate আসতে পারে** |
| **Exactly-once** | at-least-once + **idempotent consumer** (dedup) | কার্যত পাওয়া যায়, তবে broker একা দিতে পারে না |

**মূল কথা**: RabbitMQ নিজে দেয় **at-least-once** (default reliable setup-এ)। কড়া অর্থে "exactly-once delivery" কোনো distributed system-ই দিতে পারে না (network fail থাকবেই) — কিন্তু **exactly-once *effect*** পাওয়া যায় consumer-কে idempotent বানিয়ে (unique ID + dedup, দেখুন [Q8](./12-Interview-QA-Q1-Q8.md#q8-idempotency-কেন-দরকার-এবং-কীভাবে-implement-করবেন) ও [Q13](#q13-exactly-once-delivery-কি-rabbitmq-দিয়ে-সম্ভব))।

```js
// at-least-once সেটআপ: durable queue + persistent + manual ack + confirm
const ch = await conn.createConfirmChannel();
await ch.assertQueue('jobs', { durable: true });
ch.sendToQueue('jobs', Buffer.from(data), { persistent: true });
await ch.waitForConfirms();
// consumer: কাজ শেষে manual ack; duplicate সামলাতে idempotency → exactly-once effect
```

> **এক লাইনে**: at-most-once (হারাতে পারে, duplicate নেই), at-least-once (হারায় না, duplicate হতে পারে — RabbitMQ-এর default), exactly-once = at-least-once + idempotency।

### Q32: Prefetch count কত রাখা উচিত — কীভাবে tune করবেন?
**উত্তর**: এক নম্বর সঠিক উত্তর নেই — এটা **মেসেজ প্রসেস করতে কত সময় লাগে** তার উপর নির্ভর করে:
- **হালকা, দ্রুত মেসেজ** (কয়েক ms) → **বেশি prefetch** (৫০–১০০+) ভালো, নাহলে প্রতিটা মেসেজের জন্য network round-trip-এ consumer অলস বসে থাকবে।
- **ভারী, ধীর মেসেজ** (সেকেন্ড/মিনিট, যেমন video transcode) → **prefetch = 1**, যাতে একটা slow consumer অনেক মেসেজ আটকে না রাখে আর কাজ সমানভাবে ভাগ হয়।
- **Priority queue** → ছোট prefetch (১), নাহলে জরুরি মেসেজ পেছনে আটকে যায় ([Q6](./12-Interview-QA-Q1-Q8.md#q6-message-priority-কীভাবে-হ্যান্ডেল-করবেন))।

**অভিজ্ঞতাসূচক নিয়ম**: খুব বেশি prefetch → এক consumer সব মেসেজ টেনে নেয়, অন্যরা idle (unfair)। খুব কম prefetch → network overhead-এ throughput কমে। default (amqplib-এ prefetch না দিলে unlimited) prod-এ বিপজ্জনক — সবসময় একটা মান সেট করুন। শুরু করুন **prefetch ≈ 10–20** দিয়ে, তারপর queue latency ও consumer CPU দেখে tune করুন।

```js
ch.prefetch(20);   // fast task: শুরুর একটা সুস্থ default, পরে metric দেখে বাড়ান/কমান
// ভারী কাজ হলে:
ch.prefetch(1);    // একেকটা job ভারী → fair dispatch, backlog আটকায় না
```

> **এক লাইনে**: দ্রুত মেসেজ → বেশি prefetch, ভারী মেসেজ → prefetch 1; unlimited prefetch কখনো নয় — metric দেখে tune।

### Q33: Batch / Multiple ack কীভাবে ও কখন করবেন?
**উত্তর**: প্রতিটা মেসেজের জন্য আলাদা `ack` পাঠালে অনেক network round-trip হয়। `basic.ack`-এ **`multiple: true`** দিলে ওই delivery tag **পর্যন্ত সব unacked মেসেজ একসাথে** ack হয় — high-throughput/batch consumer-এ এটা network overhead অনেক কমায়।

```python
# batch consumer — ৫০০ জমিয়ে bulk insert, তারপর একবারে সব ack
buffer = []
def on_msg(ch, method, props, body):
    buffer.append(json.loads(body))
    if len(buffer) >= 500:
        db.bulk_insert(buffer)
        ch.basic_ack(method.delivery_tag, multiple=True)  # এই tag পর্যন্ত সব একসাথে ack
        buffer.clear()
```

**সাবধানতা (ট্রেড-অফ)**: multiple-ack করার **আগেই** consumer crash করলে ওই ব্যাচের **সব** মেসেজ redeliver হবে (কারণ কোনোটাই ack হয়নি) — তাই batch বড় হলে duplicate-এর সম্ভাবনা বাড়ে, consumer অবশ্যই **idempotent** হতে হবে। সাধারণ single-message consumer-এ `multiple: false` (default) রাখাই নিরাপদ।

> **এক লাইনে**: `ack(multiple=true)` batch-এ round-trip বাঁচায়, কিন্তু crash হলে পুরো batch redeliver হয় — তাই idempotency লাগে।

### Q34: Channel-level exception হলে কী হয়? Channel কখন বন্ধ হয়?
**উত্তর**: RabbitMQ-তে error দুই ধরনের — **channel-level** আর **connection-level**:
- **Channel-level error**: শুধু ওই **channel** বন্ধ হয়, connection বেঁচে থাকে। যেমন — এমন exchange-এ publish করা যেটা নেই, বা যে queue-তে permission নেই সেখানে access, বা passive declare করা queue না থাকা। error এলে RabbitMQ channel-টা close করে দেয়।
- **Connection-level error**: পুরো connection (সব channel সহ) বন্ধ হয় — যেমন protocol violation, auth fail।

**তাৎপর্য**: একটা channel বন্ধ হলে ওই channel-এর সব কাজ থেমে যায়, তাই আপনাকে channel-এর `error`/`close` event শুনে **নতুন channel তৈরি** করতে হয়। এজন্যই best practice — এক connection-এ **কাজভেদে আলাদা channel**, আর একটা channel অনেক thread-এ শেয়ার করবেন না (channel thread-safe নয়)।

```js
ch.on('error', (err) => console.error('CHANNEL error:', err.message)); // channel বন্ধ হচ্ছে
ch.on('close', () => { /* নতুন channel বানিয়ে consumer আবার সেট করুন */ });

// ভুল: নেই এমন exchange-এ publish → channel-level exception → channel বন্ধ
// তাই publish করার আগে assertExchange করে নিশ্চিত হওয়া ভালো
```

> **এক লাইনে**: channel-error শুধু channel বন্ধ করে (connection নয়) — তাই error/close শুনে নতুন channel বানান, আর channel থ্রেডে শেয়ার করবেন না।

### Q35: Production cluster কীভাবে zero-downtime upgrade করবেন?
**উত্তর**: মূল কৌশল — **rolling upgrade**: cluster-এর নোডগুলো **একটা একটা করে** upgrade করুন, বাকিরা চালু থাকে বলে ক্লায়েন্ট বিচ্ছিন্ন হয় না।

**ধাপগুলো:**
1. একটা নোড drain করুন (নতুন connection বন্ধ, client অন্য নোডে যায় — load balancer/multi-node connection URL সাহায্য করে)।
2. নোডটা stop → upgrade → start → cluster-এ আবার যোগ দিক ও sync হোক।
3. পরের নোডে যান — একসাথে একটার বেশি নোড নামাবেন না।

**কেন এটা নিরাপদে কাজ করে**: **Quorum Queue/Stream** Raft-ভিত্তিক, তাই একটা নোড down থাকলেও majority বেঁচে থাকে — কোনো মেসেজ হারায় না, write চলতে থাকে। (Classic non-replicated queue এই সুবিধা পায় না — তাই critical queue quorum রাখা জরুরি।)

**আরও যা মাথায় রাখবেন:**
- **Feature flags** — নতুন version-এ যাওয়ার আগে সব নোডে required feature flag enable আছে কিনা দেখুন।
- **Version compatibility** — সব নোড কাছাকাছি version-এ রাখুন; মাঝপথে বেশিদিন mixed-version cluster চালাবেন না।
- **বড় (major) version jump** — অনেক সময় rolling সম্ভব হয় না; তখন **blue-green**: নতুন cluster দাঁড় করিয়ে **Shovel/Federation** দিয়ে মেসেজ migrate করে traffic switch করা হয়।
- upgrade-এর আগে **definitions export** (`rabbitmqctl export_definitions`) করে backup রাখুন।

```bash
# এক নোড rolling upgrade (বাকিরা চালু থাকে)
rabbitmqctl stop_app          # এই নোড cluster থেকে সরে
# → package/image upgrade →
rabbitmqctl start_app         # আবার cluster-এ যোগ, quorum queue নিজে re-sync করে
rabbitmqctl cluster_status    # সব নোড ফিরেছে কিনা নিশ্চিত হয়ে পরের নোডে যান
```

> **এক লাইনে**: zero-downtime = একটা একটা নোড rolling upgrade + critical queue quorum (majority বাঁচে) + বড় jump-এ blue-green/Shovel migration।

---

---

[⬅ 13-Interview-QA-Q9-Q22.md](./13-Interview-QA-Q9-Q22.md) | [Kafka: 01-Introduction-and-Real-World-Examples.md ➡](../02-Kafka/01-Introduction-and-Real-World-Examples.md) | [🏠 Repo Home](../README.md)
