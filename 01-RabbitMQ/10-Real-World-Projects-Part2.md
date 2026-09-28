# Real-World Projects (১০–১৮) — সমস্যা ও সমাধান কোডসহ

### সমস্যা ১০ — [IoT Platform] লক্ষ সেন্সর থেকে টেলিমেট্রি ইনজেশন

**Project type**: IoT / Smart Device Telemetry (smart meter, fleet GPS, factory sensor)

**সমস্যা**: লক্ষ লক্ষ device প্রতি কয়েক সেকেন্ডে ছোট ছোট reading পাঠায় (temperature, voltage) — DB-তে সরাসরি লিখলে DB ধসে পড়ে।

**সমাধান**: Device → RabbitMQ → **batch-inserting consumer**। Consumer অনেক message জমিয়ে একবারে bulk insert করে; slow হলে **lazy queue** দিয়ে disk-এ backlog রাখে যাতে RAM overflow না হয়।

```python
channel.queue_declare('sensor.readings', durable=True,
    arguments={'x-queue-mode': 'lazy'})   # backlog disk-এ, RAM বাঁচে
channel.basic_qos(prefetch_count=500)     # একসাথে ৫০০ ধরে batch করি

buffer = []
def on_msg(ch, method, props, body):
    buffer.append(json.loads(body))
    if len(buffer) >= 500:
        db.bulk_insert(buffer)                       # একবারে ৫০০ row
        ch.basic_ack(method.delivery_tag, multiple=True)  # batch ack
        buffer.clear()
```

#### 🔍 আরও গভীরে — Batch insert + lazy queue দিয়ে DB বাঁচানো

- **কেন DB ধসে পড়ে**: লক্ষ device × প্রতি কয়েক সেকেন্ডে একটা করে reading = সেকেন্ডে হাজার হাজার INSERT। প্রতিটা আলাদা INSERT = আলাদা transaction, index update, disk flush — DB দমবন্ধ।
- **Batching কীভাবে বাঁচায়**: consumer মেসেজ সাথে সাথে না লিখে একটা `buffer`-এ জমায়; ৫০০ হলে **একটা `bulk_insert`** — ৫০০ row এক transaction-এ, DB-র উপর ৫০০ গুণ কম চাপ।
- **`prefetch_count=500`**: consumer একসাথে ৫০০টা মেসেজ ধরে রাখতে পারে, তাই ৫০০-এর batch বানানো সম্ভব।
- **`multiple=True` ack**: ৫০০টা আলাদা ack না পাঠিয়ে, শেষ মেসেজের delivery_tag দিয়ে **একবারে সব ৫০০** ack — network round-trip ৫০০ থেকে ১-এ নামে (Q33)।
- **`x-queue-mode: lazy`**: consumer পিছিয়ে পড়লে লক্ষ মেসেজ RAM-এ জমে broker crash করাতে পারে; lazy queue সেগুলো **disk-এ** রাখে, RAM নিরাপদ (Q25)।
- **⚠️ দুইটা gotcha**: (১) batch ack-এর আগে crash করলে পুরো ৫০০ redeliver হবে → `bulk_insert` **idempotent/upsert** হওয়া চাই। (২) ৫০০ না ভরলে শেষ কিছু মেসেজ আটকে থাকবে — একটা **time-based flush** (যেমন প্রতি ১s-এ যা আছে লিখে দাও) যোগ করা দরকার।
- **মূল শিক্ষা**: high-volume ছোট মেসেজ = **buffer করে bulk write + batch ack + lazy queue** — একটা একটা করে লিখলে ডেটাবেসই bottleneck।

### সমস্যা ১১ — [Healthcare System] ক্রিটিক্যাল অ্যালার্ট আগে

**Project type**: Hospital / Patient Monitoring System

**সমস্যা**: রুটিন notification (appointment reminder) আর জীবন-মরণ alert (patient-এর heart rate বিপজ্জনক) একই queue-তে গেলে critical alert পেছনে আটকে যেতে পারে।

**সমাধান**: **Priority queue** — critical মেসেজ সবসময় আগে process হয়।

```python
channel.queue_declare('alerts', durable=True,
    arguments={'x-max-priority': 10})     # ০–১০ priority

# critical vital alert → highest priority
channel.basic_publish('', 'alerts', json.dumps(alert),
    properties=pika.BasicProperties(priority=10, delivery_mode=2))
# routine reminder → low priority
channel.basic_publish('', 'alerts', json.dumps(reminder),
    properties=pika.BasicProperties(priority=1, delivery_mode=2))
```

#### 🔍 আরও গভীরে — Priority queue-এর ৩টি শর্ত

- **কেন FIFO যথেষ্ট নয়**: queue-তে ১০০টা routine reminder জমে থাকলে, সাধারণ FIFO-তে জীবন-মরণ vital alert ঐ ১০০টার **পেছনে** দাঁড়াবে — বিপজ্জনক। priority queue জরুরি মেসেজকে লাইন ভেঙে সামনে আনে।
- **`x-max-priority: 10`**: queue **declare করার সময়ই** সেট করতে হয়, পরে বদলানো যায় না (তখন queue delete করে আবার বানাতে হয়)। critical=10, routine=1।
- **⚠️ শর্ত ১ — শুধু backlog থাকলে কাজ করে**: consumer fast আর queue প্রায় খালি থাকলে জমে থাকার সুযোগই নেই, তাই priority-র effect দেখা যায় না। priority তখনই কাজে লাগে যখন **producer > consumer** (জট বেঁধেছে)।
- **⚠️ শর্ত ২ — prefetch ছোট রাখতে হবে**: consumer আগেই ১০০টা টেনে নিলে নতুন-আসা high-priority মেসেজ সেই ১০০টার ভেতরে ঢুকতে পারে না। তাই priority queue-তে `prefetch=1`।
- **⚠️ শর্ত ৩ — starvation নিজে সামলাতে হবে**: high-priority অবিরাম এলে low-priority **কখনোই** process হবে না। RabbitMQ এটা ঠেকায় না — level সীমিত রাখা বা আলাদা queue দিয়ে ডিজাইন করতে হয়।
- **মূল শিক্ষা**: priority queue = "backlog + ছোট prefetch + starvation-সচেতন design" — তিনটার একটা বাদ গেলে priority কাজ করে না (বিস্তারিত Q6)।

### সমস্যা ১২ — [Social Media] নোটিফিকেশন ও ফিড ফ্যান-আউট

**Project type**: Social Network / Content Platform (Facebook / Instagram টাইপ)

**সমস্যা**: একজন popular user পোস্ট করলে লক্ষ follower-কে notification/feed update দিতে হয় — sync করলে পোস্ট করাই আটকে থাকে ("fan-out on write" problem)।

**সমাধান**: পোস্ট ইভেন্ট একবার publish → **fan-out worker** follower list ভেঙে batch করে আলাদা notification queue-তে জব ফেলে, worker pool ধীরে ধীরে পাঠায়।

```js
// step 1: post → single event
ch.publish('post.events', 'post.created',
  Buffer.from(JSON.stringify({ postId, authorId })), { persistent: true });

// step 2: fan-out worker — follower-দের batch করে notification job বানায়
ch.consume('post.fanout', async (msg) => {
  const { postId, authorId } = JSON.parse(msg.content.toString());
  for (const batch of chunk(await getFollowers(authorId), 1000)) {
    ch.sendToQueue('notify.push',
      Buffer.from(JSON.stringify({ postId, userIds: batch })), { persistent: true });
  }
  ch.ack(msg);
});
```

#### 🔍 আরও গভীরে — "fan-out on write" ও celebrity problem

- **সমস্যাটা কেন কঠিন**: একজন popular user পোস্ট করলে লক্ষ follower-এর feed/notification আপডেট করতে হয়। এটা synchronously করলে "Post" বাটন চেপে ইউজার লক্ষবার DB/notification call শেষ হওয়া পর্যন্ত আটকে থাকবে।
- **দুই ধাপে সমাধান**: (১) পোস্ট হলে **একটা** `post.created` event। (২) একটা **fan-out worker** সেই event নিয়ে follower list টেনে **batch (১০০০ জন করে)** ভেঙে `notify.push` queue-তে জব ফেলে; worker pool ধীরে ধীরে পাঠায়।
- **Batch (১০০০) কেন**: প্রতি follower-এর জন্য আলাদা মেসেজ = লক্ষ মেসেজ (বিশাল overhead)। ১০০০ জনের batch = হাজার গুণ কম মেসেজ, প্রতিটা push worker একসাথে একটা batch নেয়।
- **⚠️ Celebrity problem**: কোটি-follower অ্যাকাউন্টে "fan-out on write" প্রচণ্ড ব্যয়বহুল। বড় প্ল্যাটফর্ম তখন **hybrid**: সাধারণ user-এ fan-out-on-write, celebrity-তে fan-out-on-read (follower feed চাইলে তখন pull করে) — এটা RabbitMQ-এর বাইরের architectural সিদ্ধান্ত, কিন্তু ইন্টারভিউতে বললে গভীরতা বোঝায়।
- **মূল শিক্ষা**: ভারী fan-out কাজকে "একটা event → একটা worker যে batch করে ছড়ায়" — এভাবে ভাঙলে write পাথ দ্রুত থাকে, ছড়ানোর কাজ background-এ নিয়ন্ত্রিত গতিতে হয়।

### সমস্যা ১৩ — [Fintech / Banking] রিয়েল-টাইম ফ্রড ডিটেকশন

**Project type**: Digital Wallet / Fintech (bKash / Nagad টাইপ)

**সমস্যা**: প্রতিটা transaction fraud check করা দরকার, কিন্তু sync ফ্রড-চেক করলে payment slow হয়ে যায়; আবার একই ডেটা fraud + analytics + ledger — সবার লাগে।

**সমাধান**: Transaction event **fanout** — payment মূল ফ্লো চালিয়ে যায়, আর fraud/analytics/ledger service একই event স্বাধীনভাবে consume করে। সন্দেহজনক হলে fraud service আলাদা action queue-তে ফেলে।

```js
await ch.assertExchange('txn.events', 'fanout', { durable: true });
for (const svc of ['fraud', 'analytics', 'ledger']) {
  await ch.assertQueue(`txn.${svc}`, { durable: true });
  await ch.bindQueue(`txn.${svc}`, 'txn.events', '');
}
// payment সফল হওয়ামাত্র event ছাড়ে — তিন service parallel-এ কাজ করে
ch.publish('txn.events', '', Buffer.from(JSON.stringify(txn)), { persistent: true });
```

#### 🔍 আরও গভীরে — payment block না করে fraud check

- **মূল টানাপোড়েন**: প্রতিটা transaction-এ fraud check দরকার, কিন্তু sync-এ করলে payment slow হয়ে ইউজার বিরক্ত। আবার একই ডেটা fraud + analytics + ledger — তিন জায়গায় লাগে।
- **Fanout দিয়ে সমাধান**: payment সফল হওয়ামাত্র একটা `txn` event fanout হয় → `fraud`, `analytics`, `ledger` — তিনটা service **স্বাধীনভাবে, parallel-এ** consume করে। payment-এর মূল ফ্লো এদের জন্য অপেক্ষা করে না।
- **সন্দেহজনক হলে**: fraud service নিজে decide করে একটা আলাদা `action` queue-তে জব ফেলে (account freeze / manual review / OTP challenge)।
- **⚠️ Trade-off — eventual detection**: এই ডিজাইনে fraud detection payment-এর **পরে** ঘটে, তাই খারাপ transaction হয়তো আগেই পাস হয়ে যায় → পরে reversal/hold লাগতে পারে। যদি payment-এর *আগেই* block দরকার হয়, তাহলে একটা দ্রুত **inline sync check** (বা low-latency fast path) মূল ফ্লোতে রাখতে হবে — সব fraud check async করা যায় না।
- **মূল শিক্ষা**: fanout দিয়ে "একই ঘটনা বহু দল স্বাধীনভাবে দেখুক" সহজ হয়, কিন্তু কোন চেক sync (blocking) আর কোনটা async (post-facto) — সেই সিদ্ধান্ত ব্যবসায়িক ঝুঁকির উপর নির্ভর করে।

### সমস্যা ১৪ — [Logistics / Delivery] পার্সেল স্ট্যাটাস ট্র্যাকিং

**Project type**: Courier / Last-Mile Delivery (Pathao Courier / Sundarban টাইপ)

**সমস্যা**: পার্সেলের প্রতিটা status change (picked, in-transit, delivered) থেকে customer SMS, dashboard update, partner webhook — সব একসাথে করতে হয়, আর event অবশ্যই order মেনে চলতে হবে (delivered-এর আগে picked)।

**সমাধান**: প্রতিটা parcel-এর event **consistent-hash** দিয়ে একই queue-তে (order রক্ষা), সেখান থেকে fanout করে notification/webhook worker-এ।

```js
// parcel_id দিয়ে hash → একই পার্সেলের সব event একই shard, order ঠিক
ch.publish('parcel.status', parcelId,
  Buffer.from(JSON.stringify({ parcelId, status: 'in_transit', ts: Date.now() })),
  { persistent: true });
// webhook fail করলে TTL+DLX দিয়ে retry (সমস্যা ৬-এর মতো backoff)
```

#### 🔍 আরও গভীরে — দুই প্যাটার্ন একসাথে (ordering + fanout)

- **দুইটা দাবি একসাথে**: (১) status অবশ্যই **order** মানবে — `delivered` কখনো `picked`-এর আগে process হওয়া চলবে না; (২) প্রতিটা status change থেকে একসাথে SMS + dashboard + partner webhook।
- **প্রথমে ordering (consistent-hash)**: `parcelId` দিয়ে hash → একই পার্সেলের সব event একই shard queue → একই consumer → FIFO রক্ষা (সমস্যা ৩-এর মতো)। ভিন্ন পার্সেল ভিন্ন shard-এ, তাই parallel।
- **তারপর fanout**: ঐ shard থেকে event একটা fanout exchange-এ যায় → SMS/dashboard/webhook worker আলাদাভাবে পায় (সমস্যা ৮-এর মতো)।
- **Webhook fail হলে**: partner-এর সার্ভার down থাকতে পারে — তাই webhook worker-এ **TTL+DLX backoff retry** (সমস্যা ৬), যাতে সাথে সাথে না ছেড়ে দিয়ে কিছুক্ষণ পর আবার চেষ্টা করে।
- **⚠️ Gotcha**: ordering shard-প্রতি single consumer দাবি করে; fanout-এর পরের notification worker-গুলো যত খুশি scale করা যায় (ওদের order লাগে না, শুধু status ইতিমধ্যে সঠিক ক্রমে বেরিয়ে এসেছে)।
- **মূল শিক্ষা**: বাস্তব সিস্টেমে প্রায়ই একাধিক প্যাটার্ন **stack** করতে হয় — এখানে consistent-hash (order) + fanout (broadcast) + TTL/DLX (retry) একসাথে।

### সমস্যা ১৫ — [E-commerce] ফ্ল্যাশ সেল / ইনভেন্টরি ওভারসেলিং

**Project type**: E-commerce Marketplace (Daraz Flash Sale টাইপ)

**সমস্যা**: Flash sale-এ এক সেকেন্ডে হাজার অর্ডার আসে; সরাসরি DB-তে stock কমালে race condition-এ একই পণ্য oversell হয়ে যায়।

**সমাধান**: অর্ডারগুলো queue-তে **serialize** করুন — product অনুযায়ী consistent-hash দিয়ে একই product-এর অর্ডার একই consumer-এ, যে stock check + decrement atomically করে।

```js
// একই productId → একই queue → একটাই consumer stock হ্যান্ডেল করে (no race)
ch.publish('flashsale', productId,
  Buffer.from(JSON.stringify({ orderId, productId, qty })), { persistent: true });

ch.consume('flashsale.shard.3', async (msg) => {
  const { orderId, productId, qty } = JSON.parse(msg.content.toString());
  if (await decrementStockIfAvailable(productId, qty)) {
    await confirmOrder(orderId);
  } else {
    await rejectOrder(orderId, 'out_of_stock');   // buffer শেষ, কিন্তু crash নয়
  }
  ch.ack(msg);
});
```

#### 🔍 আরও গভীরে — Serialize করে race condition মারা

- **Race condition কী এখানে**: এক সেকেন্ডে একই product-এ হাজার order একসাথে "stock আছে কি?" পড়ে, সবাই "হ্যাঁ, ১টা আছে" দেখে, সবাই কমাতে যায় → **oversell** (যত আছে তার চেয়ে বেশি বিক্রি)। কারণ read আর decrement-এর মাঝে অন্যরা ঢুকে পড়ে।
- **Serialization দিয়ে সমাধান**: `productId` consistent-hash → একই product-এর **সব** order একই shard queue → **একটাই** consumer। এখন ঐ product-এ কাজ একটার পর একটা (serial) — দুইজন একসাথে stock পড়ে-কমায় না, তাই race নেই।
- **`decrementStockIfAvailable` atomic হতে হবে**: consumer serialize করলেও, decrement নিজে atomic ধাপে হওয়া ভালো — যেমন SQL `UPDATE ... SET stock=stock-1 WHERE stock>=1` (conditional), বা Redis `DECR`। রিটার্ন দেখে confirm/reject।
- **`rejectOrder(..., 'out_of_stock')`**: stock শেষ মানে সিস্টেম fail নয় — শান্তভাবে order reject, ইউজারকে "sold out" জানানো।
- **⚠️ Trade-off**: এক product = এক consumer মানে ঐ **product-এ throughput সীমিত**। কিন্তু flash sale-এ **correctness > raw speed** — oversell করে ১০০০ কাস্টমারকে refund দেওয়ার চেয়ে সামান্য ধীর হওয়া ভালো। ভিন্ন product ভিন্ন shard-এ, তাই overall system parallel-ই থাকে।
- **মূল শিক্ষা**: concurrency bug (race)-এর একটা শক্তিশালী সমাধান হলো queue দিয়ে সংঘর্ষমুখী কাজগুলোকে **একই লাইনে serialize** করা।

### সমস্যা ১৬ — [Multi-Region SaaS] ডেটাসেন্টারের মধ্যে মেসেজ রিপ্লিকেশন

**Project type**: Global SaaS / Multi-Region Backend

**সমস্যা**: On-prem/একটা region-এ generate হওয়া event আরেকটা region-এর broker-এ পাঠাতে হবে (disaster recovery বা geo-processing-এর জন্য), কিন্তু WAN link অস্থিতিশীল।

**সমাধান**: **Shovel / Federation plugin** দিয়ে broker-to-broker মেসেজ move — কোড না বদলে, শুধু কনফিগ দিয়ে এক broker-এর queue থেকে আরেক broker-এ পাঠানো হয়।

```ini
# rabbitmq.conf — Shovel: dhaka broker → singapore broker
shovel.dc_sync.src-uri  = amqp://dhaka-broker
shovel.dc_sync.src-queue = orders.export
shovel.dc_sync.dest-uri = amqp://singapore-broker
shovel.dc_sync.dest-queue = orders.import
# WAN ছিঁড়ে গেলে source queue-তে জমে থাকে, ফিরলে আবার sync হয় — কিছু হারায় না
```

#### 🔍 আরও গভীরে — Shovel vs Federation, আর WAN নিরাপত্তা

- **সমস্যাটা**: এক region/datacenter-এ তৈরি event আরেক region-এর broker-এ লাগবে (disaster recovery বা geo-processing), কিন্তু দুই DC-র মধ্যে WAN link মাঝে মাঝে ছিঁড়ে যায়।
- **কোড বদলানো লাগে না**: Shovel/Federation হলো **plugin + config** — application অজান্তেই broker-to-broker মেসেজ move হয় (Q20)।
- **Shovel**: point-to-point — এক broker-এর নির্দিষ্ট queue থেকে টেনে অন্য broker-এর queue/exchange-এ ঠেলে দেয়। সহজ, নির্দিষ্ট route-এর জন্য।
- **Federation**: exchange/queue *level*-এ link, loosely-coupled, WAN-friendly — একাধিক broker-জুড়ে মেসেজ শেয়ার, বড় topology-তে ভালো।
- **WAN ছিঁড়লে কী হয় (সবচেয়ে গুরুত্বপূর্ণ)**: source queue-তে মেসেজ **জমতে থাকে**, link ফিরলে আবার sync হয় — at-least-once, তাই **কিছু হারায় না**।
- **⚠️ Gotcha**: cross-region latency বেশি, আর reconnect-এ **duplicate** সম্ভব → destination consumer idempotent হওয়া দরকার (Q8)।
- **মূল শিক্ষা**: broker-to-broker geo-replication কোড নয়, **অপারেশনাল কনফিগ** — Shovel (সরল, point-to-point) বা Federation (নমনীয়, exchange-level)।

### সমস্যা ১৭ — [Chat / Messaging App] অফলাইন মেসেজ ডেলিভারি

**Project type**: Real-Time Chat / Messaging (WhatsApp / Messenger টাইপ)

**সমস্যা**: রিসিভার অফলাইন থাকলে মেসেজ হারানো যাবে না; অনলাইনে ফিরলে ঠিক order-এ সব পেতে হবে।

**সমাধান**: প্রতি user-এর জন্য **durable per-user queue** — receiver অফলাইন থাকলে মেসেজ queue-তে জমা থাকে, reconnect করলে ঠিক order-এ deliver হয় (single consumer per queue → FIFO)।

```js
// প্রতি user-এর নিজস্ব durable queue — অফলাইনে থাকলেও মেসেজ জমে
await ch.assertQueue(`user.inbox.${receiverId}`, { durable: true });
ch.publish('', `user.inbox.${receiverId}`,
  Buffer.from(JSON.stringify({ from: senderId, text, ts: Date.now() })),
  { persistent: true });
// user online → নিজের queue consume করে, ack দিলে তবেই মেসেজ মোছে
```

#### 🔍 আরও গভীরে — per-user durable queue, আর এর সীমা

- **দাবি**: receiver offline থাকলে মেসেজ **হারানো যাবে না**, আর online-এ ফিরলে **ঠিক order-এ** সব পেতে হবে।
- **Per-user durable queue**: প্রতি user-এর নিজস্ব `user.inbox.<id>` queue। receiver offline থাকলে মেসেজ ওখানে জমে; reconnect করলে সে নিজের queue consume করে। একটাই consumer per queue → **FIFO order** স্বাভাবিকভাবেই রক্ষিত।
- **`durable` + `persistent` কেন**: broker restart হলেও inbox আর তার জমা মেসেজ যেন থাকে — অফলাইন ইউজারের মেসেজ কখনো হারানো চলবে না।
- **ack-এর ভূমিকা**: user মেসেজ পেয়ে ack দিলে তবেই queue থেকে মোছে — delivery নিশ্চিত না হওয়া পর্যন্ত broker ধরে রাখে।
- **⚠️ বড় সীমা (সততার সাথে জানা জরুরি)**: লক্ষ-কোটি user মানে লক্ষ-কোটি queue — RabbitMQ প্রতিটা queue-এ Erlang process/মেমরি খরচ করে, তাই বিশাল স্কেলে এটা ব্যয়বহুল। বাস্তবে অনেক chat সিস্টেম মেসেজ **DB/Cassandra বা Stream**-এ রাখে, আর RabbitMQ শুধু online real-time delivery/fan-out-এ ব্যবহার করে। এটা concept বোঝার একটা সরল মডেল — production-scale-এ hybrid লাগে।
- **মূল শিক্ষা**: durable per-entity queue offline delivery + ordering সুন্দরভাবে দেয়, কিন্তু "কত queue" সেটাই এর scaling সীমা — তাই কখন queue, কখন datastore, সেটা জানা দরকার।

### সমস্যা ১৮ — [Data Pipeline / Analytics] শিডিউলড রিপোর্ট ও ব্যাচ জব

**Project type**: BI / Analytics Backend, ETL Pipeline

**সমস্যা**: রাত ২টায় হাজার হাজার merchant-এর জন্য daily report generate করতে হয় — একসাথে চালালে সার্ভার crash করে।

**সমাধান**: Scheduler শুধু প্রতিটা report-এর জন্য একটা job queue-তে ফেলে; worker pool **prefetch** দিয়ে নিয়ন্ত্রিত গতিতে ধীরে ধীরে process করে (natural rate limiting)।

```python
# cron/scheduler → শুধু job enqueue করে, নিজে ভারী কাজ করে না
for merchant_id in all_merchants:
    channel.basic_publish('', 'report.daily',
        json.dumps({"merchant_id": merchant_id, "date": today}),
        properties=pika.BasicProperties(delivery_mode=2))

# report worker: prefetch=4 → একসাথে ৪টার বেশি ভারী report চলবে না
channel.basic_qos(prefetch_count=4)
```

#### 🔍 আরও গভীরে — Queue দিয়ে "spike smoothing" ও natural rate limiting

- **কেন crash করে**: রাত ২টায় scheduler যদি সরাসরি হাজার report **একসাথে** generate করতে যায়, সব একযোগে CPU/DB/memory টানে → server ধসে পড়ে।
- **Scheduler-এর কাজ শুধু enqueue**: cron শুধু প্রতিটা merchant-এর জন্য একটা হালকা job মেসেজ queue-তে ফেলে (কয়েক ms), নিজে কোনো ভারী কাজ করে না। ভারী report-generation পুরোটাই worker-এর দায়িত্ব।
- **`prefetch_count=4` = natural rate limiting**: worker একসাথে সর্বোচ্চ ৪টা report ধরে। ফলে হাজার job জমে থাকলেও **যেকোনো মুহূর্তে সর্বোচ্চ ৪টা** ভারী কাজ চলছে — server কখনো একসাথে সব নিয়ে হাঁপায় না। queue বাকিগুলো ধরে রাখে, worker একটা শেষ করলে পরেরটা টানে।
- **গতি নিয়ন্ত্রণ**: দ্রুত চাই? worker pod বা prefetch বাড়াও। server-এর উপর কম চাপ চাই? কমাও। queue নিজেই **spike smoothing** করে — burst-কে সমান প্রবাহে রূপান্তর করে।
- **`delivery_mode=2` (persistent)**: worker মাঝপথে crash করলে report job হারায় না, redeliver হয়।
- **মূল শিক্ষা**: "একসাথে হাজার" কে "নিয়ন্ত্রিত গতিতে কয়েকটা করে" বানানোই queue + prefetch-এর মূল শক্তি — batch/scheduled কাজে এটাই crash ঠেকায়।

> **সারমর্ম**: প্রায় সব প্যাটার্নের মূল কথা একটাই — **কাজটাকে queue-তে ফেলে দাও, মূল request দ্রুত ছেড়ে দাও, আর background worker নিজের গতিতে নিরাপদে (durable + ack + retry + DLQ) কাজ শেষ করুক।** শুধু project-এর প্রয়োজন অনুযায়ী প্যাটার্ন বদলায় — order দরকার হলে consistent-hash, broadcast দরকার হলে fanout, নিয়ন্ত্রিত গতি দরকার হলে prefetch, নিরাপত্তা দরকার হলে persistent + confirm + DLQ।

---

[⬅ 09-Real-World-Projects-Part1.md](./09-Real-World-Projects-Part1.md) | [11-Interview-Questions-All-Levels.md ➡](./11-Interview-Questions-All-Levels.md) | [🏠 Repo Home](../README.md)
