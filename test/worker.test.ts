import type { Job } from "bullmq";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	createQueueModuleFactory,
	defineJob,
	type JobOf,
	onWorkerEvent,
	processJob,
} from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
import type { JobConstructor } from "../lib/worker.decorator.js";
import { createAppFixture } from "./create-app.js";
import { mocks } from "./setup.js";

const { createApp, closeApps } = createAppFixture();

afterEach(async () => {
	try {
		await closeApps();
	} finally {
		vi.restoreAllMocks();
	}
});

class Transcode extends defineJob<{ source: string }>()({
	queueName: "audio",
	jobName: "transcode",
}) {}
class Normalize extends defineJob<{ gain: number }>()({
	queueName: "audio",
	jobName: "normalize",
}) {}

let QueueModule: ReturnType<typeof createQueueModuleFactory>;
const job = { id: "job-1", name: "transcode", data: { source: "file.wav" } };

beforeEach(() => {
	QueueModule = createQueueModuleFactory({ connection: {} });
	vi.spyOn(console, "error").mockImplementation(() => {});
});

describe("processor registration", () => {
	it.each([undefined, true, false])(
		"merges worker options and applies autorun=%s",
		async (autorun) => {
			const connection = { host: "configured" };
			const ConfiguredModule = createQueueModuleFactory({
				connection,
				prefix: "factory",
				workerOptions: { concurrency: 1, lockDuration: 30_000, autorun },
			});
			const options = {
				concurrency: 4,
				autorun: true,
				connection: { host: "ignored" },
				prefix: "ignored",
			};
			class Consumer {
				@processJob(Transcode, options)
				enabled() {}
				@processJob(Transcode, { autorun: false })
				disabled() {}
				@processJob(Transcode)
				inherited() {}
			}
			const app = createApp({
				imports: [
					ConfiguredModule({ enqueueable: {}, processable: [Transcode] }),
				],
				controllers: [Consumer],
			});

			await app.init();

			const [enabled, disabled, inherited] = mocks.workers;

			expect(enabled.options).toEqual({
				...options,
				connection,
				prefix: "factory",
				lockDuration: 30_000,
				autorun: false,
			});
			expect(disabled.options).toEqual(inherited.options);
			expect(inherited.options).toEqual({
				connection,
				prefix: "factory",
				concurrency: 1,
				lockDuration: 30_000,
				autorun: false,
			});
			expect(enabled.run).toHaveBeenCalledOnce();
			expect(disabled.run).not.toHaveBeenCalled();
			expect(inherited.run).toHaveBeenCalledTimes(autorun === false ? 0 : 1);
		},
	);

	it.each([Transcode, Normalize])(
		"rejects stacked processor decorators for %s",
		(OtherJob) => {
			expect(() => {
				class Consumer {
					@processJob(Transcode)
					@processJob(OtherJob)
					process(_job: JobOf<typeof Transcode | typeof Normalize>) {}
				}
				return Consumer;
			}).toThrow("cannot have multiple @processJob decorators");
		},
	);

	it("allows a subclass to redecorate an overridden processor", async () => {
		const process = vi.fn();
		class Base {
			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode | typeof Normalize>) {}
		}
		class Consumer extends Base {
			// biome-ignore lint/complexity/noUselessConstructor: Awilix parses the transpiled implicit rest constructor as a dependency.
			constructor() {
				super();
			}
			@processJob([Transcode, Normalize])
			process(value: JobOf<typeof Transcode | typeof Normalize>) {
				process(value);
			}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: {}, processable: [Transcode, Normalize] }),
			],
			controllers: [Consumer],
		});

		await app.init();

		expect(mocks.workers).toHaveLength(1);
		await mocks.workers[0].processor(job);
		expect(process).toHaveBeenCalledWith(job);
	});

	it.each([false, true])(
		"rejects combined processor and event roles, processor first=%s",
		async (processorFirst) => {
			class ProcessorFirst {
				@processJob(Transcode)
				@onWorkerEvent("process", "active")
				process(_job: JobOf<typeof Transcode>) {}
			}
			class EventFirst {
				@onWorkerEvent("process", "active")
				@processJob(Transcode)
				process(_job: JobOf<typeof Transcode>) {}
			}
			const app = createApp({
				imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
				controllers: [processorFirst ? ProcessorFirst : EventFirst],
			});

			await expect(app.init()).rejects.toThrow(
				"cannot be both a processor and a worker event handler",
			);
			expect(mocks.workers).toHaveLength(0);
		},
	);

	it("rejects duplicate job names before starting a worker", async () => {
		class ConflictingJob extends defineJob<{ id: number }>()({
			queueName: "audio",
			jobName: "transcode",
		}) {}
		class Controller {
			@processJob([Transcode, ConflictingJob] as readonly [
				JobConstructor,
				JobConstructor,
			])
			execute() {}
		}
		const app = createApp({
			imports: [
				QueueModule({
					enqueueable: {},
					processable: [Transcode, ConflictingJob],
				}),
			],
			controllers: [Controller],
		});

		await expect(app.init()).rejects.toThrow("Duplicate job name");
		expect(mocks.workers).toHaveLength(0);
	});
});

describe("worker events", () => {
	it("does not accept another event handler as a processor target", async () => {
		class Consumer {
			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {}
			@onWorkerEvent("process", "ready")
			onReady() {}
			@onWorkerEvent("onReady", "closed")
			onClosed() {}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await expect(app.init()).rejects.toThrow("not a @processJob method");
		expect(mocks.workers).toHaveLength(0);
	});

	it("binds handlers before startup and forwards completion arguments", async () => {
		const ready = vi.fn();
		const completed = vi.fn();
		class Consumer {
			@onWorkerEvent("process", "ready")
			onReady() {
				ready();
			}

			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {
				return 42;
			}

			@onWorkerEvent("process", "completed")
			onCompleted(
				value: JobOf<typeof Transcode>,
				result: number,
				prev: string,
			) {
				completed(value, result, prev);
			}
		}

		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await app.init();

		const worker = mocks.workers[0];
		await vi.waitFor(() => expect(ready).toHaveBeenCalledOnce());
		worker.emit("completed", job, 42, "active");
		await vi.waitFor(() =>
			expect(completed).toHaveBeenCalledWith(job, 42, "active"),
		);
	});

	it("binds stacked worker-event decorators to the same handler", async () => {
		const observed = vi.fn();
		class Consumer {
			@processJob(Transcode)
			process() {}

			@onWorkerEvent("process", "paused")
			@onWorkerEvent("process", "drained")
			onEvent() {
				observed();
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await app.init();

		mocks.workers[0].emit("paused");
		await vi.waitFor(() => expect(observed).toHaveBeenCalledOnce());
		mocks.workers[0].emit("drained");
		await vi.waitFor(() => expect(observed).toHaveBeenCalledTimes(2));
	});

	it("targets one method when workers compete on the same queue", async () => {
		const first = vi.fn();
		const second = vi.fn();
		class Consumer {
			@processJob(Transcode)
			first(_job: JobOf<typeof Transcode>) {}

			@processJob(Transcode)
			second(_job: JobOf<typeof Transcode>) {}

			@onWorkerEvent("first", "active")
			firstActive(value: JobOf<typeof Transcode>) {
				first(value);
			}
			@onWorkerEvent("second", "active")
			secondActive(value: JobOf<typeof Transcode>) {
				second(value);
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await app.init();

		mocks.workers[0].emit("active", job, "waiting");
		await vi.waitFor(() => expect(first).toHaveBeenCalledWith(job));
		expect(second).not.toHaveBeenCalled();
		mocks.workers[1].emit("active", job, "waiting");
		await vi.waitFor(() => expect(second).toHaveBeenCalledWith(job));
	});

	it("keeps inherited processor methods isolated by controller registration", async () => {
		const observed: string[] = [];
		class Base {
			label = "base";
			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {}
			@onWorkerEvent("process", "active")
			onActive() {
				observed.push(this.label);
			}
		}
		class First extends Base {
			label = "first";
		}
		class Second extends Base {
			label = "second";
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [First, Second],
		});

		await app.init();

		mocks.workers[0].emit("active", job, "waiting");
		await vi.waitFor(() => expect(observed).toEqual(["first"]));
		mocks.workers[1].emit("active", job, "waiting");
		await vi.waitFor(() => expect(observed).toEqual(["first", "second"]));
	});

	it.each(["missing", "helper"])(
		"rejects invalid processor target %s before creating workers",
		async (target) => {
			class Consumer {
				@processJob(Transcode)
				process(_job: JobOf<typeof Transcode>) {}

				helper() {}

				@onWorkerEvent(target as "helper", "active")
				onActive() {}
			}
			const app = createApp({
				imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
				controllers: [Consumer],
			});

			await expect(app.init()).rejects.toThrow("not a @processJob method");
			expect(mocks.workers).toHaveLength(0);
		},
	);

	it("resolves scoped consumers separately for processing and each event", async () => {
		let nextId = 0;
		const processed: number[] = [];
		const observed: number[] = [];
		class Consumer {
			readonly id = ++nextId;

			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {
				processed.push(this.id);
			}

			@onWorkerEvent("process", "active")
			onActive(_job: JobOf<typeof Transcode>) {
				observed.push(this.id);
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [{ useClass: Consumer, lifetime: "SCOPED" }],
		});

		await app.init();

		await mocks.workers[0].processor(job);
		mocks.workers[0].emit("active", job, "waiting");
		mocks.workers[0].emit("active", job, "waiting");
		expect(processed).toEqual([1]);
		await vi.waitFor(() => expect(observed).toEqual([2, 3]));
	});
});

describe("lifecycle", () => {
	it.each([false, true])(
		"logs handler failures without failing processing, async=%s",
		async (asynchronous) => {
			const error = new Error("event failed");
			class Consumer {
				@processJob(Transcode)
				process(_job: JobOf<typeof Transcode>) {
					return 42;
				}

				@onWorkerEvent("process", "active")
				onActive() {
					if (asynchronous) return Promise.reject(error);
					throw error;
				}
			}
			const app = createApp({
				imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
				controllers: [Consumer],
			});

			await app.init();

			mocks.workers[0].emit("active", job, "waiting");
			await vi.waitFor(() =>
				expect(console.error).toHaveBeenCalledWith(
					'[Queue] worker event handler failed for "active" on processor "process"',
					error,
				),
			);
			await expect(mocks.workers[0].processor(job)).resolves.toBe(42);
		},
	);

	it("drains async event handlers before application resources shut down", async () => {
		let finish!: () => void;
		const pending = new Promise<void>((resolve) => {
			finish = resolve;
		});
		const started = vi.fn();
		const resourceShutdown = vi.fn();
		class Resource {
			onShutdown = resourceShutdown;
		}
		class Consumer {
			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {}

			@onWorkerEvent("process", "active")
			async onActive() {
				started();
				await pending;
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
			providers: { resource: Resource },
		});

		await app.init();

		app.scope.resolve("resource");
		mocks.workers[0].emit("active", job, "waiting");
		await vi.waitFor(() => expect(started).toHaveBeenCalledOnce());
		const shutdown = app.close();
		await vi.waitFor(() =>
			expect(mocks.workers[0].close).toHaveBeenCalledOnce(),
		);
		expect(resourceShutdown).not.toHaveBeenCalled();
		finish();
		await shutdown;
		expect(resourceShutdown).toHaveBeenCalledOnce();
		expect(mocks.workers[0].listenerCount("active")).toBe(0);
	});

	it("honors autorun false after binding listeners", async () => {
		const PausedModule = createQueueModuleFactory({
			connection: {},
			workerOptions: { autorun: false },
		});
		const active = vi.fn();
		class Consumer {
			@processJob(Transcode)
			process(_job: JobOf<typeof Transcode>) {}

			@onWorkerEvent("process", "active")
			onActive() {
				active();
			}
		}
		const app = createApp({
			imports: [PausedModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await app.init();

		expect(mocks.workers[0].run).not.toHaveBeenCalled();
		expect(mocks.workers[0].listenerCount("active")).toBe(1);
	});

	it("drains workers before closing queues and application resources", async () => {
		let finish!: () => void;
		const pending = new Promise<void>((resolve) => {
			finish = resolve;
		});
		const resourceShutdown = vi.fn();
		class Resource {
			onShutdown = resourceShutdown;
		}
		class Controller {
			@processJob(Transcode)
			execute() {}
			@processJob(Transcode)
			another() {}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: { Transcode }, processable: [Transcode] }),
			],
			providers: { resource: Resource },
			controllers: [Controller],
		});

		await app.init();

		app.scope.resolve("resource");
		await app.scope
			.resolve<BullMqService<{ Transcode: typeof Transcode }>>("bullMq")
			.add(new Transcode({ source: "file.wav" }));
		mocks.workers[1].close.mockReturnValue(pending);
		const shutdown = app.close();
		await vi.waitFor(() => {
			expect(mocks.workers).toHaveLength(2);
			for (const worker of mocks.workers) {
				expect(worker.close).toHaveBeenCalledOnce();
			}
		});
		expect(mocks.queues[0].close).not.toHaveBeenCalled();
		expect(resourceShutdown).not.toHaveBeenCalled();
		finish();
		await shutdown;
		expect(mocks.queues[0].close).toHaveBeenCalledOnce();
		expect(resourceShutdown).toHaveBeenCalledOnce();
	});
});

describe("processor dispatch", () => {
	class SendEmail extends defineJob<{ to: string }>()({
		queueName: "email",
		jobName: "send-email",
		defaultJobOptions: { attempts: 3, removeOnComplete: true },
	}) {}
	class ResizeImage extends defineJob<{ url: string }>()({
		queueName: "image",
		jobName: "resize-image",
	}) {}

	class SendDigest extends defineJob<{ recipients: string[] }>()({
		queueName: "email",
		jobName: "send-digest",
	}) {}

	const connection = { host: "127.0.0.1", port: 6379 };

	beforeEach(() => {
		QueueModule = createQueueModuleFactory({
			connection,
			workerOptions: { concurrency: 4 },
		});
	});

	it("creates one worker for a list of job types sharing a queue", async () => {
		const work = vi.fn();
		class Controller {
			@processJob([SendEmail, SendDigest])
			execute(job: Job<SendEmail["payload"] | SendDigest["payload"]>) {
				return work(job);
			}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: {}, processable: [SendEmail, SendDigest] }),
			],
			controllers: [Controller],
		});

		await app.init();

		expect(mocks.workers).toHaveLength(1);
		expect(mocks.workers[0].name).toBe("email");

		for (const job of [
			{ name: "send-email", data: { to: "user" } },
			{ name: "send-digest", data: { recipients: ["user"] } },
		]) {
			await mocks.workers[0].processor(job);
			expect(work).toHaveBeenLastCalledWith(job);
		}
	});

	it.each([
		{
			jobs: [SendEmail, ResizeImage] as const,
			processable: [SendEmail, ResizeImage],
			error: "must share a queue",
		},
		{
			jobs: [SendEmail, SendDigest] as const,
			processable: [SendEmail],
			error: "not processable",
		},
		{ jobs: [] as never, processable: [], error: "at least one job" },
	])(
		"rejects invalid processor lists: $error",
		async ({ jobs, processable, error }) => {
			class Controller {
				@processJob(jobs)
				execute() {}
			}
			const app = createApp({
				imports: [QueueModule({ enqueueable: {}, processable })],
				controllers: [Controller],
			});

			await expect(app.init()).rejects.toThrow(error);
			expect(mocks.workers).toHaveLength(0);
		},
	);
});
