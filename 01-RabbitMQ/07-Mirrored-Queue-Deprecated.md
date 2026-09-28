# Mirrored Queue — পুরনো ও Deprecated পদ্ধতি


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

---

[⬅ 06-High-Availability-Quorum-Raft.md](./06-High-Availability-Quorum-Raft.md) | [08-RabbitMQ-vs-Kafka.md ➡](./08-RabbitMQ-vs-Kafka.md) | [🏠 Repo Home](../README.md)
