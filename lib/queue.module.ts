import { createModule, type Module, type ModuleDef } from "awilixify";
import type {
	BackendFactory,
	ConnectionOptions,
	IQueueBackend,
	QueueEventsOptions,
	QueueOptions,
	WorkerOptions,
} from "bullmq";
import { BullMqService } from "./bullmq.service.js";
import { BullMqRegistry } from "./bullmq-registry.js";
import { QueueEventInitializer } from "./queue-event.initializer.js";
import type { JobConstructor } from "./worker.decorator.js";
import { WorkerInitializer } from "./worker.initializer.js";

// ─── Root module types ────────────────────────────────────────────────────────

export type BullMqOptions<TConnection = ConnectionOptions> = {
	connection: NoInfer<TConnection>;
	backendFactory?: BackendFactory<IQueueBackend, TConnection>;
	/** Runs synchronously before consumer invocation. Throw an error to fail the job. */
	validate?: (JobType: JobConstructor, payload: unknown) => undefined;
	prefix?: string;
	queueOptions?: Omit<
		QueueOptions<NoInfer<TConnection>>,
		"connection" | "prefix"
	>;
	queueEventsOptions?: Omit<
		QueueEventsOptions<NoInfer<TConnection>>,
		"connection" | "prefix"
	>;
	workerOptions?: Omit<
		WorkerOptions<NoInfer<TConnection>>,
		"connection" | "prefix"
	>;
};

type BullMqRootModuleDef<TConnection = ConnectionOptions> = ModuleDef<{
	providers: {
		bullMqOptions: BullMqOptions<TConnection>;
		bullMqRegistry: BullMqRegistry<TConnection>;
	};
	exportKeys: ["bullMqRegistry", "bullMqOptions"];
}>;

export type RootDeps<TConnection = ConnectionOptions> =
	BullMqRootModuleDef<TConnection>["deps"];

// ─── Feature module types ─────────────────────────────────────────────────────

export type QueueModuleConfig<
	TEnqueueable extends Record<string, JobConstructor> = Record<
		string,
		JobConstructor
	>,
	TProcessable extends readonly JobConstructor[] = readonly JobConstructor[],
> = {
	enqueueable: TEnqueueable;
	processable: TProcessable;
};

type QueueModuleDef<
	TEnqueueable extends Record<string, JobConstructor> = Record<
		string,
		JobConstructor
	>,
	TConnection = ConnectionOptions,
> = ModuleDef<{
	providers: {
		enqueueableJobs: TEnqueueable;
		processableJobs: readonly JobConstructor[];
		bullMq: BullMqService<TEnqueueable, TConnection>;
	} & Record<string, object | string | boolean | number>;
	exportKeys: ["bullMq"];
	imports: [Module<BullMqRootModuleDef<TConnection>>];
	initializers: {
		processJob: typeof WorkerInitializer<TConnection>;
		onQueueEvent: typeof QueueEventInitializer<TConnection>;
	};
	exportInitializerKeys: ["processJob", "onQueueEvent"];
}>;

export type Deps<
	TConnection = ConnectionOptions,
	TEnqueueable extends Record<string, JobConstructor> = Record<
		string,
		JobConstructor
	>,
> = QueueModuleDef<TEnqueueable, TConnection>["deps"];

let nextId = 0;

export function createQueueModuleFactory<TConnection = ConnectionOptions>(
	options: BullMqOptions<TConnection>,
) {
	const root = createModule<BullMqRootModuleDef<TConnection>>({
		name: `BullMqRootModule_${++nextId}`,
		providers: {
			bullMqOptions: options,
			bullMqRegistry: {
				useClass: BullMqRegistry<TConnection>,
				lifetime: "SINGLETON",
				eager: true,
			},
		},
		exports: ["bullMqRegistry", "bullMqOptions"],
	});

	return function QueueModule<const TConfig extends QueueModuleConfig>(
		config: TConfig,
	) {
		return createModule<QueueModuleDef<TConfig["enqueueable"], TConnection>>({
			name: `QueueModule_${++nextId}`,
			imports: [root],
			providers: {
				enqueueableJobs: config.enqueueable,
				processableJobs: config.processable,
				bullMq: {
					useClass: BullMqService<TConfig["enqueueable"], TConnection>,
					lifetime: "SINGLETON",
				},
			},
			initializers: {
				processJob: WorkerInitializer<TConnection>,
				onQueueEvent: QueueEventInitializer<TConnection>,
			},
			exports: ["bullMq"],
			initializerExports: ["processJob", "onQueueEvent"],
		});
	};
}
