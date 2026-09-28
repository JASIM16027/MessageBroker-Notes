# আরও গভীর ইন্টারভিউ প্রশ্ন-উত্তর (Q9–Q22)


এই প্রশ্নগুলো mid থেকে senior লেভেলের ইন্টারভিউতে বেশি আসে — architecture, edge case আর operational দিক নিয়ে।

### Q9: Virtual Host (vhost) কী এবং কেন দরকার?
**উত্তর**: vhost হলো একটা RabbitMQ সার্ভারের ভেতরে **logical isolation** — প্রতিটা vhost-এর নিজস্ব আলাদা exchange, queue, binding আর permission থাকে। একটা vhost-এর queue অন্য vhost থেকে দেখা যায় না।

**কেন দরকার**: একই RabbitMQ cluster-এ multiple team বা multiple environment (dev/staging/prod) চালাতে চাইলে vhost দিয়ে আলাদা করা হয় — যাতে একটার queue আরেকটার সাথে conflict না করে। যেমন `/payments` vhost আর `/notifications` vhost সম্পূর্ণ আলাদা।

```js
// connection URL-এর শেষে vhost দেওয়া হয় (এখানে "payments")
const conn = await amqp.connect('amqp://user:pass@localhost:5672/payments');
// এই connection-এর সব queue/exchange শুধু /payments vhost-এই থাকবে, /notifications থেকে অদৃশ্য

// CLI দিয়ে vhost বানানো ও permission দেওয়া:
//   rabbitmqctl add_vhost payments
//   rabbitmqctl set_permissions -p payments myuser ".*" ".*" ".*"
```

### Q10: Publisher Confirms আর Transactions — পার্থক্য কী? কোনটা ব্যবহার করবেন?
**উত্তর**:
- **Transactions (`tx.select`/`tx.commit`)**: প্রতিটা publish একটা transaction-এ মোড়ানো হয়, commit না করা পর্যন্ত মেসেজ pending থাকে। এটা **খুব slow** (প্রতিটা commit-এ round-trip লাগে)।
- **Publisher Confirms**: Producer async-ভাবে publish করতে থাকে, RabbitMQ প্রতিটা মেসেজের জন্য পরে একটা `ack` (বা `nack`) পাঠায়। অনেক দ্রুত, কারণ batch/pipeline করা যায়।

**Best practice**: reliability দরকার হলে Publisher Confirms ব্যবহার করুন, transaction নয় — ১০x+ বেশি throughput পাওয়া যায়।

```js
// Publisher Confirms — recommended
const ch = await conn.createConfirmChannel();       // confirm mode চালু
ch.publish('ex', 'key', Buffer.from('data'), { persistent: true });
await ch.waitForConfirms();                          // RabbitMQ সত্যিই পেয়েছে কিনা নিশ্চিত

// Transactions — slow, সাধারণত avoid
// await ch.txSelect();
// ch.publish(...); await ch.txCommit();             // প্রতিটা commit = round-trip
```

### Q11: Message TTL — per-queue vs per-message পার্থক্য কী?
**উত্তর**:
- **Per-queue TTL** (`x-message-ttl` queue argument): ওই queue-এর সব মেসেজের জন্য একই expiry।
- **Per-message TTL** (`expiration` property): প্রতিটা মেসেজে আলাদা করে সময় সেট করা যায়।

**সূক্ষ্ম ব্যাপার**: RabbitMQ শুধু queue-এর **head**-এর মেসেজ expire হয়েছে কিনা চেক করে। তাই per-message TTL-এ পেছনের মেসেজ আগে expire হলেও, সামনের মেসেজ না সরা পর্যন্ত সেটা delete হয় না — এটা interview-এ tricky follow-up।

```js
// Per-queue TTL — এই queue-এর সব মেসেজ ৬০ সেকেন্ডে expire
await ch.assertQueue('temp', { arguments: { 'x-message-ttl': 60000 } });

// Per-message TTL — শুধু এই মেসেজ ১০ সেকেন্ডে expire
ch.sendToQueue('temp', Buffer.from('data'), { expiration: '10000' });  // string, ms
```

### Q12: Delayed / Scheduled message কীভাবে পাঠাবেন? (যেমন "৩০ মিনিট পর reminder")
**উত্তর**: দুইটা উপায়:
1. **Dead Letter + TTL trick**: একটা "delay queue" বানান যার consumer নেই, `x-message-ttl` সেট করা, আর `x-dead-letter-exchange` মূল queue-এ point করা। মেসেজ TTL শেষে DLX দিয়ে আসল queue-তে চলে আসে।
2. **`rabbitmq-delayed-message-exchange` plugin**: এটাই cleaner — মেসেজে `x-delay` header দিলে exchange নিজেই ওই সময় পর্যন্ত ধরে রাখে।

**Real world**: abandoned cart email (৩০ মিনিট পর), subscription renewal reminder, retry with backoff।

```js
// উপায় ১: TTL + DLX trick — delay queue-তে consumer নেই, TTL শেষে DLX দিয়ে আসল queue-তে যায়
await ch.assertQueue('reminder.delay', {
  arguments: {
    'x-message-ttl': 30 * 60 * 1000,           // ৩০ মিনিট
    'x-dead-letter-exchange': 'reminder.ready'  // এখানে আসল consumer বসে থাকে
  }
});
ch.sendToQueue('reminder.delay', Buffer.from(data));

// উপায় ২: delayed-message plugin (cleaner) — মেসেজে x-delay header
ch.publish('delayed.ex', 'key', Buffer.from(data), { headers: { 'x-delay': 1800000 } });
```

### Q13: Exactly-once delivery কি RabbitMQ দিয়ে সম্ভব?
**উত্তর**: কড়া অর্থে **না**। RabbitMQ **at-least-once** দেয় (network fail, redelivery-র কারণে duplicate আসতে পারে)। "Exactly-once processing" বাস্তবে অর্জন করা হয় **at-least-once delivery + idempotent consumer** দিয়ে — অর্থাৎ duplicate এলেও consumer একই ফল দেয় (dedup key/DB check দিয়ে)। এটা একটা খুব common "gotcha" প্রশ্ন।

```js
// exactly-once "delivery" নেই, কিন্তু exactly-once "effect" পাওয়া যায় idempotency দিয়ে
ch.consume('payments', async (msg) => {
  const { txnId } = JSON.parse(msg.content.toString());
  if (await db.exists('processed', txnId)) { ch.ack(msg); return; }  // duplicate → skip
  await db.transaction(async (t) => {
    await doWork(msg, t);
    await t.insert('processed', { txnId });     // কাজ + dedup রেকর্ড একই atomic txn-এ
  });
  ch.ack(msg);
});
// বিস্তারিত Q8 দেখুন
```

### Q14: `basic.reject` আর `basic.nack` — পার্থক্য কী?
**উত্তর**: দুটোই মেসেজ reject করে, কিন্তু:
- `basic.reject` — একবারে **একটা** মেসেজ reject করতে পারে।
- `basic.nack` — RabbitMQ-এর extension, `multiple: true` দিয়ে **একসাথে অনেক** মেসেজ reject করা যায়।

দুটোতেই `requeue` ফ্ল্যাগ আছে — `requeue=false` দিলে মেসেজ DLQ-তে যায় (থাকলে), নাহলে drop হয়।

```js
// reject — একটা মেসেজ, requeue=false → DLQ/drop
ch.reject(msg, false);

// nack — multiple=true দিয়ে এই deliveryTag পর্যন্ত সব একসাথে reject
ch.nack(msg, true, false);   // (msg, multiple, requeue)

// requeue=true দিলে আবার queue-তে ফিরবে (retry-র জন্য, কিন্তু poison message-এ সাবধান)
```

### Q15: Prefetch-এ `global` flag-এর মানে কী?
**উত্তর**: `basic.qos`-এ prefetch count দুইভাবে কাজ করে:
- **per-consumer** (default): প্রতিটা consumer আলাদাভাবে সর্বোচ্চ N মেসেজ পায়।
- **global=true**: পুরো **channel**-এর জন্য সম্মিলিতভাবে সর্বোচ্চ N।

সাধারণত per-consumer prefetch-ই চাই, যাতে fast consumer বেশি কাজ পায় আর slow consumer কম — **fair dispatch**।

```js
ch.prefetch(10);          // per-consumer (default): প্রতিটা consumer আলাদা করে ১০টা
ch.prefetch(10, true);    // global=true: পুরো channel মিলিয়ে সর্বোচ্চ ১০টা
```

### Q16: Connection ছিঁড়ে গেলে কী হয়? Automatic recovery কীভাবে কাজ করে?
**উত্তর**: Network glitch-এ connection/channel বন্ধ হয়ে যেতে পারে। বেশিরভাগ client library-তে **automatic connection recovery** থাকে — connection ফিরে এলে channel, queue, binding, consumer আবার নিজে থেকে declare করে নেয়। সাথে **heartbeat** (default ৬০ সেকেন্ড) দিয়ে RabbitMQ আর client পরস্পরকে "জীবিত আছি" জানায়; heartbeat miss হলে connection dead ধরে নেওয়া হয়।

**গুরুত্বপূর্ণ**: recovery-র সময় unacked মেসেজ redeliver হবে — তাই আবারও consumer idempotent হওয়া লাগে।

```js
// heartbeat সেট + automatic recovery (amqplib-এ reconnect নিজে wrap করতে হয়,
// বা amqp-connection-manager লাইব্রেরি ব্যবহার করা হয়)
const conn = await amqp.connect('amqp://localhost?heartbeat=60');  // ৬০s heartbeat
conn.on('error', (e) => console.error('connection error:', e.message));
conn.on('close', () => setTimeout(start, 2000));   // ছিঁড়ে গেলে reconnect চেষ্টা
// recovery-র পর queue/binding/consumer আবার declare করতে হয়
```

### Q17: RabbitMQ-তে Memory / Disk alarm কী?
**উত্তর**: RabbitMQ একটা **flow control** ব্যবস্থা — যখন RAM ব্যবহার একটা threshold (`vm_memory_high_watermark`, default ৪০%) ছাড়ায় বা free disk কমে যায়, তখন সে **publisher-দের block** করে দেয় (নতুন মেসেজ নেওয়া থামিয়ে দেয়), যাতে সার্ভার crash না করে। Consumer কাজ চালিয়ে যায়, backlog কমলে আবার publisher খুলে যায়। Production issue debug করতে এটা জানা জরুরি।

```js
// client-এ টের পাওয়া যায় — block হলে publish আটকে থাকবে
conn.on('blocked',   (reason) => console.warn('publisher BLOCKED:', reason));  // alarm উঠেছে
conn.on('unblocked', ()       => console.info('publisher unblocked'));         // ঠিক হয়েছে

// config (rabbitmq.conf) — threshold টিউন করা:
//   vm_memory_high_watermark.relative = 0.4
//   disk_free_limit.absolute = 2GB
```

### Q18: Quorum Queue আর Classic Queue — কখন কোনটা?
**উত্তর**:
- **Quorum Queue**: data safety + HA দরকার (payment, order) — Raft দিয়ে replicated, no data loss।
- **Classic Queue**: non-critical, ephemeral, বা খুব high-throughput temporary কাজ (যেমন per-client RPC reply queue) — হালকা, কিন্তু single-node, replicate হয় না।

Mirrored (HA classic) queue এখন deprecated — নতুন প্রজেক্টে HA লাগলে Quorum।

```js
// Quorum — data safety + HA (payment, order)
await ch.assertQueue('orders', {
  durable: true, arguments: { 'x-queue-type': 'quorum' }
});

// Classic — হালকা, non-critical / temporary (যেমন RPC reply queue)
await ch.assertQueue('rpc.reply', { exclusive: true });   // default = classic
```

### Q19: Competing Consumers আর Pub/Sub pattern-এর পার্থক্য RabbitMQ-তে কীভাবে হয়?
**উত্তর**:
- **Competing Consumers (work queue)**: একটা queue, অনেক consumer — প্রতিটা মেসেজ **একজনই** পায় (load sharing)। Default direct/queue behavior।
- **Pub/Sub (fanout)**: fanout exchange-এ একাধিক queue bind করা, প্রতিটা queue-র নিজস্ব consumer — একই মেসেজ **সবাই** পায় (broadcast)।

মূল কৌশল: "মেসেজ একজন নেবে" চাইলে এক queue শেয়ার করান; "সবাই নেবে" চাইলে প্রত্যেকের আলাদা queue বানান।

```js
// Competing Consumers — একই queue-এ অনেক consumer, প্রতিটা মেসেজ একজনই পায়
await ch.assertQueue('tasks', { durable: true });
ch.consume('tasks', handler);   // এই worker কয়েকটা instance-এ চালান → load ভাগ

// Pub/Sub — fanout exchange, প্রত্যেকের আলাদা queue → সবাই একই মেসেজ পায়
await ch.assertExchange('events', 'fanout');
const { queue } = await ch.assertQueue('', { exclusive: true });  // এই consumer-এর নিজস্ব queue
await ch.bindQueue(queue, 'events', '');
ch.consume(queue, handler);
```

### Q20: Shovel আর Federation plugin কী কাজে লাগে?
**উত্তর**: দুটোই **broker-to-broker** মেসেজ move করার জন্য (যেমন এক datacenter থেকে আরেকটায়):
- **Shovel**: এক queue থেকে মেসেজ টেনে অন্য broker-এর exchange/queue-তে পাঠায় — point-to-point, সহজ কনফিগ।
- **Federation**: exchange/queue level-এ link — একাধিক broker-জুড়ে মেসেজ শেয়ার, WAN-friendly (loose coupling)।

**Use case**: multi-region deployment, on-prem থেকে cloud-এ migration, geo-distributed system।

```ini
# Shovel — এক broker-এর queue থেকে আরেক broker-এ point-to-point টেনে নেয় (rabbitmq.conf)
shovel.my-shovel.src-uri  = amqp://src-broker
shovel.my-shovel.src-queue = orders
shovel.my-shovel.dest-uri = amqp://dest-broker
shovel.my-shovel.dest-queue = orders-copy

# Federation — exchange/queue level link, WAN-friendly (upstream সেট করে policy দিয়ে বাঁধা হয়):
#   rabbitmqctl set_parameter federation-upstream up1 '{"uri":"amqp://remote-broker"}'
#   rabbitmqctl set_policy fed "^events\." '{"federation-upstream-set":"all"}'
```

### Q21: Poison message কী এবং কীভাবে handle করবেন?
**উত্তর**: যে মেসেজ কখনোই সফলভাবে process হয় না (malformed data, permanent bug) — বারবার fail করে requeue হয়ে queue আটকে দেয়, এটাই **poison message**। সমাধান: `x-death` header দিয়ে retry count track করা, নির্দিষ্ট সংখ্যক fail-এর পর **DLQ**-তে সরিয়ে দেওয়া এবং alert তোলা — মূল pipeline সচল রাখা।

```js
// main queue-এ DLQ বাঁধা → reject হলে মেসেজ DLQ-তে যায়, x-death এ count বাড়ে
await ch.assertQueue('jobs', {
  durable: true, arguments: { 'x-dead-letter-exchange': 'jobs.dlx' }
});

ch.consume('jobs', (msg) => {
  const deaths = msg.properties.headers?.['x-death']?.[0]?.count || 0;
  if (deaths >= 3) { moveToParkingLot(msg); ch.ack(msg); alert(msg); return; } // poison → সরাও
  try { process(msg); ch.ack(msg); }
  catch { ch.nack(msg, false, false); }   // requeue=false → DLQ, x-death count বাড়বে
});
```

### Q22: RabbitMQ কীভাবে monitor করবেন production-এ?
**উত্তর**:
- **Management Plugin** (web UI + HTTP API): queue depth, message rate, consumer count, memory।
- **Prometheus + Grafana**: `rabbitmq_prometheus` plugin দিয়ে metrics scrape করে dashboard/alert।
- মূল যে metric-গুলো watch করবেন: **queue length (backlog)**, **unacked message count**, **consumer utilisation**, **memory/disk alarm**, **redelivery rate**।

```bash
# Management HTTP API দিয়ে queue-এর অবস্থা দেখা (alerting script-এ কাজে লাগে)
curl -u guest:guest http://localhost:15672/api/queues/%2F/orders \
  | jq '{ready: .messages_ready, unacked: .messages_unacknowledged, consumers: .consumers}'

# CLI দিয়ে দ্রুত দেখা:
#   rabbitmqctl list_queues name messages messages_unacknowledged consumers
```

---

---

[⬅ 12-Interview-QA-Q1-Q8.md](./12-Interview-QA-Q1-Q8.md) | [14-Interview-QA-Q23-Q35.md ➡](./14-Interview-QA-Q23-Q35.md) | [🏠 Repo Home](../README.md)
