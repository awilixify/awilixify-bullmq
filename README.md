# @awilixify/bullmq

Module-scoped BullMQ 6 jobs for Awilixify

```sh
npm install @awilixify/bullmq bullmq awilixify awilix ioredis
```

## Usage

```ts
import { DIContext, type ModuleDef } from "awilixify";
import {
  createQueueModuleFactory,
  defineJob,
  type JobOf,
  onWorkerEvent,
  processJob,
} from "@awilixify/bullmq";

class SendEmail extends defineJob<{ to: string }>()({
  queueName: "email",
  jobName: "send-email",
  defaultJobOptions: { attempts: 3 },
}) {}

const QueueModule = createQueueModuleFactory({
  connection: { host: "localhost", port: 6379 },
  workerOptions: { concurrency: 4 },
});
const EmailModule = QueueModule({
  enqueueable: { SendEmail },
  processable: [SendEmail],
});
type EmailDeps = ModuleDef<{ imports: [typeof EmailModule] }>["deps"];
const EMAIL_WORKER = Symbol("email-worker");

class EmailConsumer {
  @processJob(SendEmail, { workerId: EMAIL_WORKER })
  process(job: JobOf<typeof SendEmail>) {
    console.log(job.data.to);
    return "sent";
  }

  @onWorkerEvent("process", "completed")
  onCompleted(job: JobOf<typeof SendEmail>, result: string) {
    console.log(job.id, result);
  }
}

const app = DIContext.create({
  name: "App",
  imports: [EmailModule],
  controllers: [EmailConsumer],
});
await app.init();

const bullMq = app.scope.resolve<EmailDeps["bullMq"]>("bullMq");
await bullMq.add(new SendEmail({ to: "user@example.com" }), { attempts: 5 });

// During application shutdown:
// await app.close();
```

Inject `EmailDeps["bullMq"]` as a constructor dependency named `bullMq` to use it in services.

## Contracts

- `enqueueable` exposes constructors through `bullMq.jobs` and permits `add()` and `getQueue()`. `processable` permits local `@processJob` handlers. Use an empty object or list for consumer-only or producer-only modules.
- Reuse one factory across feature modules to share queues and identified workers within an application. Separate applications and factories remain isolated.
- Use stable `queueName` and `jobName` values. Each decorated processor creates a worker. Workers on the same queue compete for jobs; job names do not route jobs to different methods.
- `add()` merges class defaults with per-call options and returns the enqueued BullMQ job. It does not wait for processing. Handlers receive the full native `Job` and return the processing result.
- Processor options override factory `workerOptions`. Set `connection` and `prefix` only on the factory. `autorun: false` leaves a worker stopped until you call `run()`.
- `onWorkerEvent` targets a decorated processor on the same controller. Arguments are checked against its job and result types. Event-handler failures are logged, not propagated to the processor.
- Scoped controllers get a fresh instance per job or event invocation. Shutdown drains workers and pending event handlers before closing application resources; pending jobs remain queued. Handlers must not await application shutdown themselves.

### Native access

```ts
const queue = bullMq.getQueue(SendEmail);
await queue.addBulk([{ name: "send-email", data: { to: "user@example.com" } }]);

const worker = bullMq.getWorker(EMAIL_WORKER);
await worker.pause();
await worker.resume();
```

`getQueue()` requires an enqueueable constructor, then grants unrestricted queue-level access, bypassing class payload typing and class defaults. `getWorker()` requires a unique symbol ID and is available after `app.init()`. Reuse the same symbol instance. `app.close()` closes these resources.

Flows and sandboxed processors have no managed integration. Use BullMQ directly for those features. Native QueueEvents instances are not exposed, and decorated processors receive only the job, not additional processor callback arguments.

### Multiple job types

```ts
class SendDigest extends defineJob<{ recipients: string[] }>()({
  queueName: "email",
  jobName: "send-digest",
}) {}

class EmailConsumer {
  @processJob([SendEmail, SendDigest])
  process(job: JobOf<typeof SendEmail | typeof SendDigest>) {
    if (job.name === "send-email") console.log(job.data.to);
    else console.log(job.data.recipients);
  }
}
```

Register both classes in `processable`. Use one decorator per method, with a non-empty list of jobs sharing a queue and having distinct job names. Without validation, unknown job names are also passed to the handler. All workers sharing that queue must understand its jobs.

## Queue-wide events

```ts
import { onQueueEvent, type QueueEventOf } from "@awilixify/bullmq";

class EmailObserver {
  @onQueueEvent("email", "completed")
  completed(event: QueueEventOf<"completed">, streamId: string) {
    console.log(event.jobId, event.returnvalue, streamId);
  }
}
```

Register the observer as a controller in a module importing `QueueModule({ enqueueable: {}, processable: [] })`. Queue events work without local workers. They receive event data, not full jobs; results are `unknown`. Set observer options through factory `queueEventsOptions`.

## Validation

Payload typing does not validate runtime data. TypeBox support is optional:

```sh
npm install @sinclair/typebox
```

```ts
import { Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";
import { UnrecoverableError } from "bullmq";
import {
  jobWithSchema,
  type SchemaJobConstructor,
} from "@awilixify/bullmq/typebox";

class ProcessOrder extends jobWithSchema(Type.Object({ id: Type.Number() }), {
  queueName: "orders",
  jobName: "process-order",
}) {}

const QueueModule = createQueueModuleFactory({
  connection: { host: "localhost", port: 6379 },
  validate(JobType: SchemaJobConstructor, payload) {
    if (JobType.schema && !Value.Check(JobType.schema, payload)) {
      throw new UnrecoverableError("Invalid payload");
    }
  },
});
```

`jobWithSchema` infers the payload and attaches `.schema`; it does not validate by itself. The callback runs synchronously before processing, never during enqueueing. Throw to reject data; return nothing on success. It cannot transform the payload. The example skips plain jobs without schemas.

With validation enabled, unknown job names fail before the callback. `UnrecoverableError` prevents retries; other errors follow BullMQ retry options.

## Backends

Redis is the default and requires an application-installed driver such as `ioredis`. For PostgreSQL, install `pg` instead and configure:

```ts
import { createPostgresBackend } from "bullmq";

const QueueModule = createQueueModuleFactory({
  backendFactory: createPostgresBackend,
  connection: { database: "jobs", migrate: true },
});
```

The backend determines the connection type and applies to queues, workers, and observers. This package does not install database drivers.

## Development

```sh
npm ci
npm run build:release
```

Runtime tests mock BullMQ and require no database.
