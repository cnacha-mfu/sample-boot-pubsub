# Live coding plan — your first event-driven system

The library, rebuilt around **events**: one service announces, two services
react, nobody calls anybody.

Everything structural is done. What is left are the `// TODO` markers, in the
order we will do them. Search the project for `TODO` to find them all.

---

## Where the answers are

The finished version is in
[`cnacha-mfu/sample-boot-pubsub-solution`](https://github.com/cnacha-mfu/sample-boot-pubsub-solution).
Every step below links straight to the file that answers it.

---

## Before we start

```bash
docker compose up            # ZooKeeper + Kafka. Leave this terminal open
mvn install -DskipTests      # from THIS folder
```

Then three more terminals:

```bash
mvn -pl library-borrow-service        spring-boot:run    # 8200
mvn -pl library-notification-service  spring-boot:run    # 8201
mvn -pl library-popularity-service    spring-boot:run    # 8202
```

Open side by side: <http://localhost:8201/> and <http://localhost:8202/>.

Check your work at any time:

```bash
mvn test         # 12 tests; -fae to see every module, not just the first
```

Or import `postman/library-pubsub.postman_collection.json` into Postman.

---

## The one idea behind all five steps

> Last time, transaction-service **asked** book-service and **waited** for the
> answer - and when book-service was down, everything failed.
>
> Today, borrow-service **announces** "a borrow happened" on a **topic** and
> moves on. The **broker** keeps the announcement. Whoever subscribed gets it -
> now, or when they come back. The producer never knows who is listening, and
> never has to.

---

# The five steps

## Step 1 — publish the event (10 min)

**File:** `library-borrow-service/.../BorrowController.java`

> 💡 **Solution:**
> [`BorrowController.java`](https://github.com/cnacha-mfu/sample-boot-pubsub-solution/blob/main/library-borrow-service/src/main/java/th/mfu/borrow/BorrowController.java)

One line:

```java
kafkaTemplate.send(topicName, borrowJson);
```

`KafkaTemplate` is the producer's whole API, the way `KafkaListener` will be
the consumer's. `topicName` is `"borrows"` - a **topic** is a named channel
inside the broker; events go in at one end, subscribers read them out at the
other.

Restart borrow-service and post a borrow:

```bash
curl -X POST http://localhost:8200/borrows \
  -H "Content-Type: application/json" \
  -d '{"bookTitle":"1984","memberName":"Alice Johnson"}'
```

**Now look at the event sitting in the broker.** Nobody has consumed it yet -
it is just lying in the log:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic borrows --from-beginning
```

Leave that window open for the rest of the session - every event you publish
will scroll past in it.

**Say out loud:** the send line did not wait, and it could not fail because a
subscriber was down. There are no subscribers. It went to the broker, and the
broker keeps it.

✅ `placingABorrowPublishesTheEvent` passes.

---

## Step 2 — the first subscriber (15 min)

**File:** `library-notification-service/.../BorrowPlacedListener.java`

> 💡 **Solution:**
> [`BorrowPlacedListener.java`](https://github.com/cnacha-mfu/sample-boot-pubsub-solution/blob/main/library-notification-service/src/main/java/th/mfu/notification/BorrowPlacedListener.java)

```java
@KafkaListener(topics = "${app.kafka.topic:borrows}", groupId = "notification-group")
```

Two things the annotation says:

- `topics` — **which** channel to read;
- `groupId` — **who is reading**. Remember it for step 3.

Then the body: read the JSON with `objectMapper`, build a `Notification`, save
it. Nobody calls this method - the broker delivers to it.

Restart notification-service and watch two things at once:

1. it immediately processes the borrows you posted in step 1 - **events wait
   for their subscribers**;
2. post a new borrow and <http://localhost:8201/> shows it within two seconds.

✅ `theListenerSubscribesWithItsOwnGroup`, `anEventBecomesANotification`,
`everyEventBecomesItsOwnNotification` pass.

---

## Step 3 — the second subscriber, on your own (10 min)

**File:** `library-popularity-service/.../BorrowPlacedListener.java`

> 💡 **Solution:**
> [`BorrowPlacedListener.java`](https://github.com/cnacha-mfu/sample-boot-pubsub-solution/blob/main/library-popularity-service/src/main/java/th/mfu/popularity/BorrowPlacedListener.java)

Same pattern, from memory if you can. Same topic — but the group id is
**`popularity-group`**, and the reaction is different: find the row for this
book title, add one to its count.

Post one borrow and watch **both pages change**. The producer sent the event
**once**. Both services got it, because they subscribed as **different
groups** - each group receives its own copy of every event.

**The question to ask the class:** what would happen if this service reused
`notification-group`? (Kafka would treat the two services as two copies of the
*same* subscriber and split the stream between them - each event to only one.
Same group = sharing the work; different group = everyone gets everything.
There is no code for this - it is all in that one string.)

✅ all 12 tests pass — the rest of the session is watching it work.

---

## Step 4 — stop a subscriber, lose nothing (10 min) ⭐

No code. This is the payoff of the whole architecture.

1. **Stop notification-service** (Ctrl-C in its terminal).
2. Post two or three borrows. **borrow-service still answers 201.** The
   popularity page still updates. Nothing anywhere failed.
3. Restart notification-service and watch its log: it processes every borrow it
   missed, in order, and its page catches up.

**Put the microservice sample on the screen next to this.** Last time, one
stopped service meant a 503 from its caller within two seconds. Today a
stopped service means *nothing happens until it comes back, and then everything
happens*. The broker turned "you must be up when I need you" into "read it when
you can".

That is what **decoupled** means on the slide: not just "no compile-time
dependency", but *my failure is not your failure*.

---

## Step 5 — rebuild state from the log (10 min) ⭐

No code, one property change. This is **event sourcing** made visible.

Look at the popularity page: those counts are **derived** from the events.
Nobody ever posted a count. So the table is disposable — prove it:

1. **Stop popularity-service.** Its H2 database is in memory, so its table of
   counts is now *gone*.
2. Restart it — and the counts are **still gone** (its group already read the
   topic, so nothing is redelivered; an empty table is what "caught up" looks
   like for a new database).
3. Now change one line in `library-popularity-service/.../application.properties` —
   give the listener a **new group id** in the annotation, or simply edit
   `groupId = "popularity-group"` to `"popularity-group-2"` — and restart.
4. The service replays **every borrow event since the beginning** (that is what
   `auto-offset-reset=earliest` means for a brand-new group), and the counts
   rebuild themselves, exactly as they were.

**The idea on the slide, now on the screen:** the topic is an **append-only
log** and it is the *truth*. Any table built from it is a summary you can throw
away and rebuild — at any time, from any point. That is also why events are
never changed, only added: change one and every rebuilt summary would silently
disagree with the past.

(Change the group id back afterwards, or leave it — it only names the reader.)

---

## Wrap-up

| Idea from the lecture | Where you touched it |
| --- | --- |
| event | the JSON that `send()` published |
| producer / publisher | borrow-service, step 1 |
| topic | `"borrows"`, steps 1–3 |
| broker | the Kafka container; the console-consumer window |
| consumer / subscriber | the two listeners, steps 2–3 |
| consumer group | the two `groupId`s, step 3 |
| decoupling | stop a subscriber, lose nothing — step 4 |
| event sourcing | replay the log, rebuild the counts — step 5 |
| ZooKeeper / KRaft | the other container; see the README note |

Then: **`lab-web-pubsub`**. Same shape, different story — orders instead of
borrows:

| In the lab | Here |
| --- | --- |
| `order-service`, `POST /orders` | `library-borrow-service`, `POST /borrows` |
| topic `orders` | topic `borrows` |
| `payment-service`, `payment-group` | `library-notification-service`, `notification-group` |
| `shipping-service`, `shipping-group` | `library-popularity-service`, `popularity-group` |
| `OrderPlacedListener` | `BorrowPlacedListener` |

Two differences to expect in the lab: **everything runs in Docker there**
(so the broker address is `kafka:9092`, not `localhost:9094` — the two doors
from the README), and the lab parses JSON with a small helper method instead of
Jackson — both are fine.

---

## Class challenge — a third listener

The library wants to know who its most active members are.

Build **library-activity-service** (port 8203): it subscribes to the same
`borrows` topic and counts borrows **per member**, the way popularity-service
counts them per book. Show the counts on `GET /activity`, or copy the little
auto-refresh page too.

Rules:

- Copy `library-popularity-service` and change what needs changing. It is the
  same pattern on purpose - you should not need any new idea.
- Choose a **new group id**. (Reuse `popularity-group` and watch what happens
  to the two services' pages. That is not a bug - explain it.)
- **Do not touch borrow-service.** It must not even be restarted.

When your service works, answer one question: **how many lines of the producer
changed to support a third subscriber?**

That number is the whole lesson. Last week, adding a caller meant the callee had
to be found, be up, and answer in time. Today, adding a subscriber is invisible
to everyone else in the system.

Extra credit: start your service LAST, after posting some borrows - does it see
the old events or only new ones? Which property decides that?

---

## If something breaks

| Symptom | Cause |
| --- | --- |
| `Connection to node -1 (localhost/127.0.0.1:9094) could not be established` | The broker is not up. `docker compose up`, and wait for it |
| The same error, but `kafka:9092` | Wrong door: from your machine it is `localhost:9094`. Check `KAFKA_BOOTSTRAP_SERVERS` is not set in your shell |
| Port 2181 or 9092 already in use | The lab's containers (or another compose stack) are running. `docker compose down` there first |
| Posting works but nothing appears on a page | That service's listener is not subscribed yet (step 2/3), or the service is down. Check its log for `received from topic` |
| A page shows old data after you restarted its service | Its H2 database is in memory — restart empties it. That is step 5's starting point, not a bug |
| Counts doubled after replay | You replayed into a table that already had the counts. Restart the service first (fresh in-memory DB), then replay |
| `docker compose up` is very slow the first time | It downloads the Kafka and ZooKeeper images once. After that it is fast |
