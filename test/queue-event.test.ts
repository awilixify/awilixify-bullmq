import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	createQueueModuleFactory,
	defineJob,
	onQueueEvent,
	processJob,
	type QueueEventOf,
} from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
import { BullMqShutdownError } from "../lib/errors.js";
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
let QueueModule: ReturnType<typeof createQueueModuleFactory>;
const completed = {
	jobId: "job-1",
	returnvalue: { result: 42 },
	prev: "active",
};

beforeEach(() => {
	QueueModule = createQueueModuleFactory({ connection: {}, prefix: "app" });
});

describe("queue events", () => {
	it("observes arbitrary queues", async () => {
		const work = vi.fn();
		class Observer {
			@onQueueEvent("external-audio", "completed")
			onCompleted(event: QueueEventOf<"completed">, id: string) {
				work(event, id);
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [] })],
			controllers: [Observer],
		});

		await app.init();

		expect(mocks.events).toHaveLength(1);
		expect(mocks.events[0].name).toBe("external-audio");
		expect(mocks.events[0].options).toEqual({
			connection: {},
			prefix: "app",
			autorun: false,
		});
		expect(mocks.events[0].listenersAtStart).toBe(1);
		expect(mocks.events[0].run).toHaveBeenCalledOnce();
		mocks.events[0].emit("completed", completed, "stream-1");
		await vi.waitFor(() =>
			expect(work).toHaveBeenCalledWith(completed, "stream-1"),
		);
	});

	it("shares one QueueEvents instance per queue across feature modules", async () => {
		const first = vi.fn();
		const second = vi.fn();
		class First {
			@onQueueEvent(Transcode.queueName, "completed")
			completed(event: QueueEventOf<"completed">) {
				first(event);
			}
		}
		class Second {
			@onQueueEvent("audio", "completed")
			completed(event: QueueEventOf<"completed">) {
				second(event);
			}
		}
		const app = createApp({
			imports: [
				{
					name: "First",
					imports: [QueueModule({ enqueueable: {}, processable: [] })],
					controllers: [First],
				},
				{
					name: "Second",
					imports: [QueueModule({ enqueueable: {}, processable: [] })],
					controllers: [Second],
				},
			],
		});

		await app.init();

		expect(mocks.events).toHaveLength(1);
		mocks.events[0].emit("completed", completed, "stream-1");
		await vi.waitFor(() => {
			expect(first).toHaveBeenCalledWith(completed);
			expect(second).toHaveBeenCalledWith(completed);
		});
	});

	it("supports stacked decorators observing multiple queues and event types", async () => {
		const work = vi.fn();
		class Observer {
			@onQueueEvent("audio", "paused")
			@onQueueEvent("video", "resumed")
			changed(event: object, id: string) {
				work(event, id);
			}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: {}, processable: [] })],
			controllers: [Observer],
		});

		await app.init();

		expect(mocks.events.map((events) => events.name).sort()).toEqual([
			"audio",
			"video",
		]);
		mocks.events
			.find((events) => events.name === "audio")
			.emit("paused", {}, "audio-id");
		mocks.events
			.find((events) => events.name === "video")
			.emit("resumed", {}, "video-id");
		await vi.waitFor(() => expect(work).toHaveBeenCalledTimes(2));
		expect(work).toHaveBeenCalledWith({}, "audio-id");
		expect(work).toHaveBeenCalledWith({}, "video-id");
	});

	it("forwards queue-event options without starting a paused observer", async () => {
		const PausedModule = createQueueModuleFactory({
			connection: {},
			queueEventsOptions: {
				autorun: false,
				blockingTimeout: 100,
			},
		});
		class Observer {
			@onQueueEvent("external", "completed") completed() {}
		}
		const app = createApp({
			imports: [PausedModule({ enqueueable: {}, processable: [] })],
			controllers: [Observer],
		});

		await app.init();
		expect(mocks.events[0].options).toEqual({
			connection: {},
			autorun: false,
			blockingTimeout: 100,
		});
		expect(mocks.events[0].run).not.toHaveBeenCalled();
	});

	it("starts observers before workers and closes workers, observers, then queues", async () => {
		class Observer {
			@processJob(Transcode) process() {}
			@onQueueEvent("audio", "completed") completed() {}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: { Transcode }, processable: [Transcode] }),
			],
			controllers: [Observer],
		});

		await app.init();

		app.scope
			.resolve<BullMqService<{ Transcode: typeof Transcode }>>("bullMq")
			.getQueue(Transcode);
		const [worker] = mocks.workers;
		const [observer] = mocks.events;
		const [queue] = mocks.queues;

		expect(observer.run).toHaveBeenCalledBefore(worker.run);

		await app.close();

		expect(worker.close).toHaveBeenCalledOnce();
		expect(observer.close).toHaveBeenCalledOnce();
		expect(queue.close).toHaveBeenCalledOnce();
		expect(worker.close).toHaveBeenCalledBefore(observer.close);
		expect(observer.close).toHaveBeenCalledBefore(queue.close);
		expect(observer.listenerCount("completed")).toBe(0);
	});

	it("preserves every shutdown failure and still closes producer queues", async () => {
		class Observer {
			@onQueueEvent("audio", "completed") completed() {}
		}
		const app = createApp({
			imports: [QueueModule({ enqueueable: { Transcode }, processable: [] })],
			controllers: [Observer],
		});

		await app.init();

		await app.scope
			.resolve<any>("bullMq")
			.add(new Transcode({ source: "file.wav" }));
		const errors = [
			new Error("events close failed"),
			new Error("queue close failed"),
		];
		mocks.events[0].close.mockRejectedValueOnce(errors[0]);
		mocks.queues[0].close.mockRejectedValueOnce(errors[1]);
		const failure = (await app
			.close()
			.catch((error: unknown) => error)) as AggregateError;
		expect(failure).toBeInstanceOf(AggregateError);
		const registryFailure = failure.errors.find(
			(error) => error instanceof BullMqShutdownError,
		);
		expect(registryFailure?.errors).toEqual(errors);
		expect(mocks.queues[0].close).toHaveBeenCalledOnce();
	});
});
