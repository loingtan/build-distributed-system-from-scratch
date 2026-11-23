So far the broker itself never fails. In this task it does: the tests **crash** it and start it again 150 ms later. Everything in variables is gone, including timers. Producers were told `enqueue_ok` for jobs that the starter no longer knows about.

## What you build

The same broker with the same messages, keeping its promises across a crash.

`node.disk` is a plain object that survives. After a restart the file runs again from the top with the same `node.disk`:

```js
node.disk.jobs ??= {};      // first start: create; after a restart: already there
```

It holds plain JSON only: objects, arrays, strings, numbers. A `Map` or a timer handle does not survive.

After a restart the broker must still know:

- every job that is waiting
- every job that was with a consumer. Its timer is gone, so the broker has to start a new one or put the job back in the queue
- how many times each job has been handed out, and the dead-letter list
- every id it has accepted or acknowledged, so that retries and late acks still change nothing

## Timing

While the broker is down, requests get no answer. Producers repeat an `enqueue`, and consumers repeat an `ack`, every 300 ms until answered.

The 500 ms visibility timeout is unchanged (not handed out again before 450 ms; available by 750 ms), with two allowances around a crash:

- a job that was in flight when the broker died may be handed out again early
- it must be available no later than 750 ms after the restart

The limit of 3 hand-outs has no allowance. A job on its third hand-out at the crash must not get a fourth.

## The idea

A reply is a promise. Change the disk first and reply second, and anything a client has been told is already safe. Because one handler does both without being interrupted, there is no moment where the reply is out and the disk is not written.

## The tests check

- no accepted job is lost in a crash, waiting or in flight
- acknowledged jobs stay acknowledged, and retried enqueues stay deduplicated
- the hand-out limit and the dead letters survive
- everything the previous tasks checked
