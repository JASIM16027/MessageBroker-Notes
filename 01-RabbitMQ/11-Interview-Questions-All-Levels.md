# ইন্টারভিউ প্রশ্ন — সব লেভেল


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

---

[⬅ 10-Real-World-Projects-Part2.md](./10-Real-World-Projects-Part2.md) | [12-Interview-QA-Q1-Q8.md ➡](./12-Interview-QA-Q1-Q8.md) | [🏠 Repo Home](../README.md)
