# যে সমস্যা Kafka সমাধান করে


Kafka কীভাবে কাজ করে এবং কী সমস্যা সমাধান করে, ধাপে ধাপে বুঝিয়ে দিচ্ছি।

ধরুন আপনার একটা বড় কোম্পানি — যেখানে অনেকগুলো সিস্টেম একে অপরের ডেটা চায়:

- **Order Service**-এর ডেটা লাগে: Inventory, Shipping, Analytics, Recommendation, Fraud, Email — ৬টা সিস্টেমের
- **User activity** ডেটা লাগে: Analytics, Personalization, Ad-targeting — আরও কয়েকটার

**Kafka ছাড়া (point-to-point integration)** — প্রতিটা সিস্টেমকে প্রতিটা অন্য সিস্টেমের সাথে সরাসরি জুড়তে হয়। ৬টা সিস্টেম হলে সম্ভাব্য সংযোগ প্রায় N×N — এটাকে বলে **"integration spaghetti"**:
- নতুন একটা consumer যোগ করতে হলে source service-এর কোড বদলাতে হয়
- একটা downstream slow/down হলে upstream আটকে যায়
- একই ডেটা বারবার আলাদা করে পাঠাতে হয়, কেউ পুরনো ডেটা পেতে পারে না

**Kafka দিয়ে** — Order Service শুধু একটাবার `orders` topic-এ event লেখে। যত সিস্টেম দরকার, সবাই সেই topic subscribe করে নিজের গতিতে পড়ে। Source-কে জানতেও হয় না কে কে পড়ছে। নতুন consumer যোগ করা মানে শুধু নতুন একটা consumer group — source-এ কোনো পরিবর্তন লাগে না। এটাই Kafka-র মূল শক্তি: **producer আর consumer সম্পূর্ণ decoupled, আর ডেটা log-এ থাকে বলে যেকোনো সময়, যতবার খুশি পড়া যায়।**

---

---

[⬅ 01-Introduction-and-Real-World-Examples.md](./01-Introduction-and-Real-World-Examples.md) | [03-How-It-Works-Flow.md ➡](./03-How-It-Works-Flow.md) | [🏠 Repo Home](../README.md)
