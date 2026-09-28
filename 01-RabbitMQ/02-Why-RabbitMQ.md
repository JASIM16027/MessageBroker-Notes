# যে সমস্যা RabbitMQ সমাধান করে


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

---

[⬅ 01-Introduction-and-Real-World-Examples.md](./01-Introduction-and-Real-World-Examples.md) | [03-How-It-Works-Flow.md ➡](./03-How-It-Works-Flow.md) | [🏠 Repo Home](../README.md)
