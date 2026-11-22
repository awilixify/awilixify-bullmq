import { type BackendFactory, type IQueueBackend, withBackend } from "bullmq";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createQueueModuleFactory, defineJob, processJob } from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
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

class SendEmail extends defineJob<{ to: string }>()({
	queueName: "email",
	jobName: "send-email",
	defaultJobOptions: { attempts: 3, removeOnComplete: true },
}) {}
class ResizeImage extends defineJob<{ url: string }>()({
	queueName: "image",
	jobName: "resize-image",
}) {}

const connection = { host: "127.0.0.1", port: 6379 };
let QueueModule: ReturnType<typeof createQueueModuleFactory>;

function setup(
	config = {
		enqueueable: { SendEmail },
		processable: [] as readonly (typeof SendEmail)[],
	},
) {
	const app = createApp({
		imports: [QueueModule(config)],
	});

	return {
		app,
		queue:
			app.scope.resolve<BullMqService<{ SendEmail: typeof SendEmail }>>(
				"bullMq",
			),
	};
}

beforeEach(() => {
	QueueModule = createQueueModuleFactory({
		connection,
		workerOptions: { concurrency: 4 },
	});
});

describe("enqueueing and queue access", () => {
	it("enqueues without a local worker and merges job options", async () => {
		const { app, queue } = setup();

		await app.init();

		expect(queue.jobs.SendEmail).toBe(SendEmail);
		const job = await queue.add(new SendEmail({ to: "user@example.com" }), {
			attempts: 5,
		});
		expect(job).toEqual({
			name: "send-email",
			data: { to: "user@example.com" },
			opts: { attempts: 5, removeOnComplete: true },
		});
		await queue.add(new SendEmail({ to: "second@example.com" }));
		expect(mocks.queues).toHaveLength(1);
		expect(mocks.queues[0].options).toEqual({ connection });
		expect(mocks.workers).toHaveLength(0);
	});

	it("checks module scope before enqueueing or exposing native queues", async () => {
		const { app, queue } = setup();

		await app.init();

		const Impostor = class extends SendEmail {};
		expect(() => queue.getQueue(ResizeImage as any)).toThrow("not enqueueable");
		expect(() => queue.getQueue(Impostor)).toThrow("not enqueueable");
		expect(() =>
			queue.add(new ResizeImage({ url: "image.png" }) as any),
		).toThrow("not enqueueable");
		expect(() => queue.add(new Impostor({ to: "user@example.com" }))).toThrow(
			"not enqueueable",
		);
		expect(mocks.queues).toHaveLength(0);

		const native = queue.getQueue(SendEmail);

		expect(native).toBe(mocks.queues[0]);
		expect(queue.getQueue(SendEmail)).toBe(native);
	});

	it("enqueues distinct job names on one shared queue", async () => {
		class DeliverDigest extends defineJob<{ to: string }>()({
			queueName: "email",
			jobName: "deliver-digest",
		}) {}
		const jobs = { SendEmail, DeliverDigest };
		const app = createApp({
			imports: [QueueModule({ enqueueable: jobs, processable: [] })],
		});

		await app.init();

		const queue = app.scope.resolve<BullMqService<typeof jobs>>("bullMq");
		expect(queue.getQueue(SendEmail)).toBe(queue.getQueue(DeliverDigest));

		const email = await queue.add(new SendEmail({ to: "user" }));
		const digest = await queue.add(new DeliverDigest({ to: "user" }));

		expect(email.name).toBe("send-email");
		expect(digest.name).toBe("deliver-digest");
		expect(mocks.queues).toHaveLength(1);
		expect(mocks.queues[0].name).toBe("email");
	});
});

describe("factory configuration", () => {
	it("shares the factory prefix between queues and workers", async () => {
		const prefix = "my-app";
		QueueModule = createQueueModuleFactory({ connection, prefix });
		class Controller {
			@processJob(SendEmail)
			execute() {}
		}
		const app = createApp({
			imports: [
				QueueModule({ enqueueable: { SendEmail }, processable: [SendEmail] }),
			],
			controllers: [Controller],
		});

		await app.init();
		await app.scope
			.resolve<BullMqService<{ SendEmail: typeof SendEmail }>>("bullMq")
			.add(new SendEmail({ to: "user" }));
		expect(mocks.queues[0].options.prefix).toBe(prefix);
		expect(mocks.workers[0].options.prefix).toBe(prefix);
	});

	it("binds the selected backend without changing the default backend", async () => {
		const backendFactory =
			vi.fn<BackendFactory<IQueueBackend, { database: string }>>();
		const connection = { database: "jobs" };
		const CustomQueueModule = createQueueModuleFactory({
			backendFactory,
			connection,
		});
		class Controller {
			@processJob(SendEmail)
			execute() {}
		}
		const app = createApp({
			imports: [
				CustomQueueModule({
					enqueueable: { SendEmail },
					processable: [SendEmail],
				}),
			],
			controllers: [Controller],
		});

		await app.init();

		const queue =
			app.scope.resolve<BullMqService<{ SendEmail: typeof SendEmail }>>(
				"bullMq",
			);
		await queue.add(new SendEmail({ to: "user" }));
		expect(withBackend).toHaveBeenCalledWith(backendFactory);
		expect(mocks.queues[0].options).toEqual({ connection });
		expect(mocks.workers[0].options).toEqual({ connection, autorun: false });
		vi.mocked(withBackend).mockClear();

		const defaultApp = setup();
		await defaultApp.app.init();
		await defaultApp.queue.add(new SendEmail({ to: "default" }));

		expect(withBackend).not.toHaveBeenCalled();
	});
});

describe("application isolation", () => {
	it("isolates applications using the same module factory", async () => {
		const first = setup();
		const second = setup();
		await first.app.init();
		await second.app.init();
		await first.queue.add(new SendEmail({ to: "first" }));
		await second.queue.add(new SendEmail({ to: "second" }));
		expect(mocks.queues).toHaveLength(2);
		await first.app.close();
		expect(mocks.queues[0].close).toHaveBeenCalledOnce();
		expect(mocks.queues[1].close).not.toHaveBeenCalled();
		expect(() => first.queue.add(new SendEmail({ to: "closed" }))).toThrow(
			"after shutdown",
		);
	});
});
