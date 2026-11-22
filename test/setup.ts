import { beforeEach, vi } from "vitest";

const mocks = vi.hoisted(() => ({
	queues: [] as any[],
	workers: [] as any[],
	events: [] as any[],
	order: [] as string[],
	failForQueue: undefined as string | undefined,
}));

export { mocks };

vi.mock("bullmq", async () => {
	const actual = await vi.importActual<typeof import("bullmq")>("bullmq");
	const { EventEmitter } = await import("node:events");

	const constructors = {
		Queue: class extends EventEmitter {
			add = vi.fn(async (name, data, opts) => ({ name, data, opts }));
			close = vi.fn(async () => {
				mocks.order.push("queue.close");
			});

			constructor(
				public name: string,
				public options: unknown,
			) {
				super();
				mocks.queues.push(this);
			}
		},

		Worker: class extends EventEmitter {
			get opts() {
				return this.options;
			}

			listenersAtStart = 0;

			run = vi.fn(async () => {
				this.listenersAtStart = this.listenerCount("active");
				mocks.order.push("worker.run");
			});

			close = vi.fn(async () => {
				mocks.order.push("worker.close");
				this.emit("closed");
			});

			constructor(
				public name: string,
				public processor: (job: unknown) => unknown,
				public options: unknown,
			) {
				super();

				if (name === mocks.failForQueue)
					throw new Error("Worker construction failed");

				mocks.workers.push(this);
				queueMicrotask(() => this.emit("ready"));
			}
		},

		QueueEvents: class extends EventEmitter {
			listenersAtStart = 0;

			run = vi.fn(async () => {
				this.listenersAtStart = this.listenerCount("completed");
				mocks.order.push("events.run");
			});

			close = vi.fn(async () => {
				mocks.order.push("events.close");
			});

			constructor(
				public name: string,
				public options: unknown,
			) {
				super();
				mocks.events.push(this);
			}
		},
	};

	return {
		...actual,
		...constructors,
		withBackend: vi.fn(() => constructors),
	};
});

beforeEach(() => {
	mocks.queues.length = 0;
	mocks.workers.length = 0;
	mocks.events.length = 0;
	mocks.order.length = 0;
	mocks.failForQueue = undefined;
	vi.clearAllMocks();
});
