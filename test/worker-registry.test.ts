import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createQueueModuleFactory, defineJob, processJob } from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
import {
	DuplicateWorkerIdError,
	InvalidWorkerIdError,
	RegistryShutdownError,
	WorkerNotAvailableError,
} from "../lib/errors.js";
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
class Failing extends defineJob<unknown>()({
	queueName: "failing",
	jobName: "failing",
}) {}

let QueueModule: ReturnType<typeof createQueueModuleFactory>;

beforeEach(() => {
	QueueModule = createQueueModuleFactory({ connection: {} });
});

describe("native worker access", () => {
	it("looks up an identified worker after initialization", async () => {
		const workerId = Symbol("audio-worker");
		class Consumer {
			@processJob(Transcode, { workerId, autorun: false, concurrency: 4 })
			process() {}
			@processJob(Transcode)
			unnamed() {}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		const workers = app.scope.resolve<BullMqService>("bullMq");
		expect(() => workers.getWorker(workerId)).toThrow(WorkerNotAvailableError);

		await app.init();

		const worker = workers.getWorker(workerId);
		expect(worker).toBe(mocks.workers[0]);
		expect(workers.getWorker(workerId)).toBe(worker);
		expect(worker.opts.concurrency).toBe(4);
		expect(worker.opts).not.toHaveProperty("workerId");
		expect(mocks.workers[0].run).not.toHaveBeenCalled();
		expect(mocks.workers[1].run).toHaveBeenCalledOnce();
		expect(() => workers.getWorker(Symbol("audio-worker"))).toThrow(
			WorkerNotAvailableError,
		);
		expect(() => workers.getWorker("audio-worker" as never)).toThrow(
			InvalidWorkerIdError,
		);

		await app.close();

		expect(() => workers.getWorker(workerId)).toThrow(RegistryShutdownError);
		for (const value of mocks.workers)
			expect(value.close).toHaveBeenCalledOnce();
	});

	it.each([false, true])(
		"rejects duplicate IDs before creating workers, separate features=%s",
		async (separateFeatures) => {
			const workerId = Symbol("duplicate");
			class First {
				@processJob(Transcode, { workerId }) process() {}
			}
			class Second {
				@processJob(Transcode, { workerId }) process() {}
			}
			class Combined {
				@processJob(Transcode, { workerId }) first() {}
				@processJob(Transcode, { workerId }) second() {}
			}
			const app = createApp(
				separateFeatures
					? {
							imports: [
								{
									name: "First",
									imports: [
										QueueModule({ enqueueable: {}, processable: [Transcode] }),
									],
									controllers: [First],
								},
								{
									name: "Second",
									imports: [
										QueueModule({ enqueueable: {}, processable: [Transcode] }),
									],
									controllers: [Second],
								},
							],
						}
					: {
							imports: [
								QueueModule({ enqueueable: {}, processable: [Transcode] }),
							],
							controllers: [Combined],
						},
			);

			await expect(app.init()).rejects.toThrow(DuplicateWorkerIdError);
			expect(mocks.workers).toHaveLength(0);
		},
	);

	it("isolates the same symbol between applications and factories", async () => {
		const workerId = Symbol("audio");
		class Consumer {
			@processJob(Transcode, { workerId }) process() {}
		}
		const createIsolatedApp = (Factory: typeof QueueModule) =>
			createApp({
				imports: [Factory({ enqueueable: {}, processable: [Transcode] })],
				controllers: [Consumer],
			});
		const first = createIsolatedApp(QueueModule);
		const second = createIsolatedApp(QueueModule);
		const third = createIsolatedApp(
			createQueueModuleFactory({ connection: {} }),
		);

		await first.init();
		await second.init();
		await third.init();

		const firstWorkers = first.scope.resolve<BullMqService>("bullMq");
		const secondWorkers = second.scope.resolve<BullMqService>("bullMq");
		const thirdWorkers = third.scope.resolve<BullMqService>("bullMq");
		expect(firstWorkers.getWorker(workerId)).toBe(mocks.workers[0]);
		expect(secondWorkers.getWorker(workerId)).toBe(mocks.workers[1]);
		expect(thirdWorkers.getWorker(workerId)).toBe(mocks.workers[2]);

		await first.close();

		expect(() => firstWorkers.getWorker(workerId)).toThrow(
			RegistryShutdownError,
		);
		expect(secondWorkers.getWorker(workerId)).toBe(mocks.workers[1]);
	});

	it("rejects non-symbol IDs from untyped callers", async () => {
		class Consumer {
			@processJob(Transcode, { workerId: "invalid" as never }) process() {}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [Transcode] })],
			controllers: [Consumer],
		});

		await expect(app.init()).rejects.toThrow(InvalidWorkerIdError);
		expect(mocks.workers).toHaveLength(0);
	});

	it("does not expose partially initialized workers after startup fails", async () => {
		const workerId = Symbol("audio");
		mocks.failForQueue = "failing";

		class Consumer {
			@processJob(Transcode, { workerId }) first() {}
			@processJob(Failing) second() {}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: {}, processable: [Transcode, Failing] }),
			],
			controllers: [Consumer],
		});

		await expect(app.init()).rejects.toThrow("Worker construction failed");
		expect(mocks.workers).toHaveLength(1);
		expect(() =>
			app.scope.resolve<BullMqService>("bullMq").getWorker(workerId),
		).toThrow(WorkerNotAvailableError);

		await app.close();

		expect(mocks.workers[0].close).toHaveBeenCalledOnce();
	});
});
