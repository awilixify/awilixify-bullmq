import { createDecoratorStateUpdater } from "awilixify";
import type { Job, JobsOptions, WorkerListener, WorkerOptions } from "bullmq";
import * as ERRORS from "./errors.js";

// ─── Job definitions ─────────────────────────────────────────────────────────

export type JobDefinition = {
	readonly queueName: string;
	readonly jobName: string;
	readonly defaultJobOptions?: JobsOptions;
};

export type JobConstructor = JobDefinition & {
	new (payload: never): { readonly payload: unknown };
};

export function defineJob<TPayload>() {
	return <const TDefinition extends JobDefinition>(definition: TDefinition) =>
		class {
			static readonly queueName: TDefinition["queueName"] =
				definition.queueName;
			static readonly jobName: TDefinition["jobName"] = definition.jobName;
			static readonly defaultJobOptions = definition.defaultJobOptions;

			constructor(public readonly payload: TPayload) {}
		};
}

export type JobOf<TJob extends JobConstructor> = TJob extends JobConstructor
	? Job<InstanceType<TJob>["payload"], unknown, TJob["jobName"]>
	: never;

// ─── Shared metadata ─────────────────────────────────────────────────────────

type WorkerMethodMetadata = {
	jobs?: readonly JobConstructor[];
	workerOptions?: ProcessorOptions;
	workerEvents: readonly {
		processorMethod: string | symbol;
		event: WorkerEvent;
	}[];
};

export const workerState: ReturnType<
	typeof createDecoratorStateUpdater<{ method: () => WorkerMethodMetadata }>
> = createDecoratorStateUpdater("BullMQ Workers", {
	method: (): WorkerMethodMetadata => ({ workerEvents: [] }),
});

// ─── Job processing ──────────────────────────────────────────────────────────

export type ProcessorOptions = Omit<WorkerOptions, "connection" | "prefix"> & {
	/** Optional identity for native worker access through the workers provider. */
	workerId?: symbol;
};

// Track declarations per class, rather than treating inherited metadata as stacking.
const processorDeclarations = new WeakMap<object, Set<string | symbol>>();

type ProcessJobInput =
	| JobConstructor
	| readonly [JobConstructor, ...JobConstructor[]];

type ConsumerJob<TInput extends ProcessJobInput> = JobOf<
	TInput extends readonly JobConstructor[]
		? TInput[number]
		: Extract<TInput, JobConstructor>
>;

type UniqueJobNames<
	TJobs extends readonly JobConstructor[],
	TSeen extends string = never,
> = TJobs extends readonly [
	infer TFirst extends JobConstructor,
	...infer TRest extends readonly JobConstructor[],
]
	? string extends TFirst["jobName"]
		? UniqueJobNames<TRest, TSeen>
		: Extract<TFirst["jobName"], TSeen> extends never
			? UniqueJobNames<TRest, TSeen | TFirst["jobName"]>
			: never
	: unknown;

export function processJob<const TInput extends ProcessJobInput>(
	input: TInput &
		(TInput extends readonly JobConstructor[]
			? UniqueJobNames<TInput>
			: unknown),
	options?: ProcessorOptions,
) {
	const jobs: readonly JobConstructor[] = Array.isArray(input)
		? [...input]
		: [input as JobConstructor];

	return <TThis, TReturn>(
		_target: (this: TThis, job: ConsumerJob<TInput>) => TReturn,
		context: ClassMethodDecoratorContext<
			TThis,
			(this: TThis, job: ConsumerJob<TInput>) => TReturn
		>,
	) => {
		workerState.update(context, {
			method: (metadata) => {
				if (context.metadata) {
					const methods =
						processorDeclarations.get(context.metadata) ??
						new Set<string | symbol>();

					if (methods.has(context.name)) {
						throw new ERRORS.MultipleProcessJobDecoratorsError(context.name);
					}

					methods.add(context.name);
					processorDeclarations.set(context.metadata, methods);
				}

				return { ...metadata, jobs, workerOptions: options };
			},
		});
	};
}

// ─── Worker events ───────────────────────────────────────────────────────────

export type WorkerEvent = keyof WorkerListener;

type ProcessorMethod = (...args: never[]) => unknown;
type ProcessorJob<TMethod extends ProcessorMethod> =
	Parameters<TMethod> extends [infer TJob, ...unknown[]]
		? TJob extends Job
			? TJob
			: Job<unknown, unknown>
		: Job<unknown, unknown>;

type ReplaceJob<TArgs extends unknown[], TJob> = {
	[TIndex in keyof TArgs]: TArgs[TIndex] extends infer TArg
		? TArg extends Job
			? TJob
			: TArg
		: never;
};

export type WorkerEventArgs<
	TProcessor extends ProcessorMethod,
	TEvent extends WorkerEvent,
> = ReplaceJob<
	Parameters<WorkerListener<unknown, Awaited<ReturnType<TProcessor>>>[TEvent]>,
	ProcessorJob<TProcessor>
>;

export function onWorkerEvent<
	const TMethod extends string | symbol,
	const TEvent extends WorkerEvent,
>(processorMethod: TMethod, event: TEvent) {
	return <TThis extends Record<TMethod, ProcessorMethod>, TReturn>(
		_target: (
			this: TThis,
			...args: WorkerEventArgs<TThis[TMethod], TEvent>
		) => TReturn,
		context: ClassMethodDecoratorContext<
			TThis,
			(this: TThis, ...args: WorkerEventArgs<TThis[TMethod], TEvent>) => TReturn
		>,
	) => {
		workerState.update(context, {
			method: (metadata) => ({
				...metadata,
				workerEvents: [...metadata.workerEvents, { processorMethod, event }],
			}),
		});
	};
}
