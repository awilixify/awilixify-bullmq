import type { AfterInit, BeforeShutdown } from "awilixify";
import {
	type BackendClasses,
	type ConnectionOptions,
	type IQueueBackend,
	type Processor,
	Queue,
	QueueEvents,
	Worker,
	withBackend,
} from "bullmq";
import * as ERRORS from "./errors.js";
import type { RootDeps } from "./queue.module.js";
import type { QueueEvent } from "./queue-event.decorator.js";
import type { ProcessorOptions, WorkerEvent } from "./worker.decorator.js";

type ProcessorRegistration = {
	target: object;
	methodName: string | symbol;
	queueName: string;
	processor: Processor<unknown, unknown, string>;
	workerOptions?: Omit<ProcessorOptions, "workerId">;
	workerId?: symbol;
};

type EventRegistration = {
	target: object;
	processorMethod: string | symbol;
	event: WorkerEvent;
	invoke: (...args: unknown[]) => unknown;
};

type QueueEventRegistration = {
	queueName: string;
	event: QueueEvent;
	invoke: (...args: unknown[]) => unknown;
};

export class BullMqRegistry<TConnection = ConnectionOptions>
	implements AfterInit, BeforeShutdown
{
	private readonly queues = new Map<
		string,
		InstanceType<BackendClasses<IQueueBackend, TConnection>["Queue"]>
	>();
	private readonly workers = new Set<
		InstanceType<BackendClasses<IQueueBackend, TConnection>["Worker"]>
	>();
	private readonly workersById = new Map<
		symbol,
		InstanceType<BackendClasses<IQueueBackend, TConnection>["Worker"]>
	>();
	private readonly queueEvents = new Map<
		string,
		InstanceType<BackendClasses<IQueueBackend, TConnection>["QueueEvents"]>
	>();
	private readonly constructors: Pick<
		BackendClasses<IQueueBackend, TConnection>,
		"Queue" | "Worker" | "QueueEvents"
	>;
	private readonly processors: ProcessorRegistration[] = [];
	private readonly eventListeners: EventRegistration[] = [];
	private readonly queueEventListeners: QueueEventRegistration[] = [];
	private readonly eventInvocations = new Set<Promise<void>>();
	private readonly listenerCleanup: (() => void)[] = [];
	private initialized = false;
	private stopping = false;

	constructor(
		private readonly bullMqOptions: RootDeps<TConnection>["bullMqOptions"],
	) {
		this.constructors = bullMqOptions.backendFactory
			? withBackend(bullMqOptions.backendFactory)
			: { Queue, Worker, QueueEvents };
	}

	public getOrCreateQueue(queueName: string) {
		if (this.stopping) throw new ERRORS.RegistryShutdownError("access queues");

		const queue = this.queues.get(queueName);

		if (queue) return queue;

		const created = new this.constructors.Queue(queueName, {
			...this.bullMqOptions.queueOptions,
			connection: this.bullMqOptions.connection,
			prefix: this.bullMqOptions.prefix,
		});

		created.on("error", (error: Error) => {
			console.error(`[Queue] queue failed for queue "${queueName}"`, error);
		});

		this.queues.set(queueName, created);

		return created;
	}

	public getWorker(workerId: symbol) {
		if (this.stopping) throw new ERRORS.RegistryShutdownError("access workers");

		if (typeof workerId !== "symbol") throw new ERRORS.InvalidWorkerIdError();

		const worker = this.workersById.get(workerId);

		if (!this.initialized || !worker)
			throw new ERRORS.WorkerNotAvailableError(workerId);

		return worker;
	}

	public registerProcessor(registration: ProcessorRegistration): void {
		if (this.stopping)
			throw new ERRORS.RegistryShutdownError("register processors");

		if (registration.workerId !== undefined) {
			if (typeof registration.workerId !== "symbol")
				throw new ERRORS.InvalidWorkerIdError();

			if (
				this.workersById.has(registration.workerId) ||
				this.processors.some(
					(processor) => processor.workerId === registration.workerId,
				)
			) {
				throw new ERRORS.DuplicateWorkerIdError(registration.workerId);
			}
		}

		this.processors.push(registration);
	}

	public registerWorkerEventListener(registration: EventRegistration): void {
		if (this.stopping)
			throw new ERRORS.RegistryShutdownError("register worker events");

		this.eventListeners.push(registration);
	}

	public registerQueueEventListener(
		registration: QueueEventRegistration,
	): void {
		if (this.stopping)
			throw new ERRORS.RegistryShutdownError("register queue events");

		this.queueEventListeners.push(registration);
	}

	afterInit(): void {
		const startWorkers: (() => Promise<void>)[] = [];

		for (const registration of this.queueEventListeners) {
			const queueEvents = this.getOrCreateQueueEvents(registration.queueName);
			const listener = this.createEventListener(
				registration.invoke,
				`[Queue] queue event handler failed for "${registration.event}" on queue "${registration.queueName}"`,
			);
			queueEvents.on(registration.event, listener);
			this.listenerCleanup.push(() =>
				queueEvents.off(registration.event, listener),
			);
		}
		this.queueEventListeners.length = 0;

		// Defer construction as well as startup so even ready events have listeners.
		for (const processor of this.processors) {
			const workerOptions = {
				...this.bullMqOptions.workerOptions,
				...processor.workerOptions,
			};
			const worker = this.registerWorker(
				processor.queueName,
				processor.processor,
				workerOptions,
			);

			if (processor.workerId !== undefined)
				this.workersById.set(processor.workerId, worker);

			if (workerOptions.autorun !== false)
				startWorkers.push(() => worker.run());

			const listeners = this.eventListeners.filter(
				(registration) =>
					registration.target === processor.target &&
					registration.processorMethod === processor.methodName,
			);

			for (const registration of listeners) {
				const listener = this.createEventListener(
					registration.invoke,
					`[Queue] worker event handler failed for "${registration.event}" on processor "${String(registration.processorMethod)}"`,
				);

				worker.on(registration.event, listener);
				this.listenerCleanup.push(() =>
					worker.off(registration.event, listener),
				);
			}
		}

		this.processors.length = 0;
		this.eventListeners.length = 0;
		this.initialized = true;

		if (this.bullMqOptions.queueEventsOptions?.autorun !== false) {
			for (const [queueName, queueEvents] of this.queueEvents) {
				void queueEvents.run().catch((error: unknown) => {
					console.error(
						`[Queue] queue events run failed for queue "${queueName}"`,
						error,
					);
				});
			}
		}

		for (const start of startWorkers) {
			void start().catch((error: unknown) => {
				console.error("[Queue] worker run failed", error);
			});
		}
	}

	private createEventListener(
		invoke: (...args: unknown[]) => unknown,
		errorMessage: string,
	) {
		return (...args: unknown[]) => {
			// Defer invocation so synchronous throws and async rejections share error handling and shutdown tracking.
			const pending = Promise.resolve()
				.then(() => invoke(...args))
				.catch((error: unknown) => {
					console.error(errorMessage, error);
				})
				.then(() => {
					this.eventInvocations.delete(pending);
				});
			this.eventInvocations.add(pending);
		};
	}

	private getOrCreateQueueEvents(queueName: string) {
		const existing = this.queueEvents.get(queueName);

		if (existing) return existing;

		const queueEvents = new this.constructors.QueueEvents(queueName, {
			...this.bullMqOptions.queueEventsOptions,
			connection: this.bullMqOptions.connection,
			prefix: this.bullMqOptions.prefix,
			autorun: false,
		});

		queueEvents.on("error", (error: Error) => {
			console.error(
				`[Queue] queue events failed for queue "${queueName}"`,
				error,
			);
		});
		this.queueEvents.set(queueName, queueEvents);

		return queueEvents;
	}

	private registerWorker(
		queueName: string,
		processor: Processor<unknown, unknown, string>,
		workerOptions: Omit<ProcessorOptions, "workerId">,
	) {
		if (this.stopping)
			throw new ERRORS.RegistryShutdownError("register workers");

		const worker = new this.constructors.Worker<unknown, unknown, string>(
			queueName,
			processor,
			{
				...workerOptions,
				autorun: false,
				connection: this.bullMqOptions.connection,
				prefix: this.bullMqOptions.prefix,
			},
		);
		worker.on("error", (error: Error) => {
			console.error(`[Queue] worker failed for queue "${queueName}"`, error);
		});
		this.workers.add(worker);

		return worker;
	}

	async beforeShutdown(): Promise<void> {
		this.stopping = true;
		// Drain handlers before queues or other application resources are closed.
		const workers = await Promise.allSettled(
			[...this.workers.values()].map((worker) => worker.close()),
		);

		const queueEvents = await Promise.allSettled(
			[...this.queueEvents.values()].map((events) => events.close()),
		);
		for (const cleanup of this.listenerCleanup.splice(0)) cleanup();
		while (this.eventInvocations.size) {
			await Promise.all([...this.eventInvocations]);
		}

		this.processors.length = 0;
		this.eventListeners.length = 0;
		this.queueEventListeners.length = 0;
		const queues = await Promise.allSettled(
			[...this.queues.values()].map((queue) => queue.close()),
		);

		this.workers.clear();
		this.workersById.clear();
		this.queues.clear();
		this.queueEvents.clear();
		const errors = [...workers, ...queueEvents, ...queues].flatMap((result) =>
			result.status === "rejected" ? [result.reason] : [],
		);

		if (errors.length) throw new ERRORS.BullMqShutdownError(errors);
	}
}
