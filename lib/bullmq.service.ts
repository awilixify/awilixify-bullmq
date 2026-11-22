import type { ConnectionOptions, Job, JobsOptions } from "bullmq";
import * as ERRORS from "./errors.js";
import type { Deps } from "./queue.module.js";
import type { JobConstructor } from "./worker.decorator.js";

export class BullMqService<
	TEnqueueable extends Record<string, JobConstructor> = Record<
		string,
		JobConstructor
	>,
	TConnection = ConnectionOptions,
> {
	public readonly jobs: TEnqueueable;
	private readonly enqueueableConstructors: readonly JobConstructor[];

	constructor(
		private readonly bullMqRegistry: Deps<TConnection>["bullMqRegistry"],
		private readonly enqueueableJobs: Deps<
			TConnection,
			TEnqueueable
		>["enqueueableJobs"],
	) {
		this.jobs = this.enqueueableJobs;
		this.enqueueableConstructors = Object.values(this.enqueueableJobs);
	}

	/** Returns the native queue, granting full queue-level access after the module scope check. */
	public getQueue(job: TEnqueueable[keyof TEnqueueable]) {
		if (!this.enqueueableConstructors.includes(job)) {
			throw new ERRORS.JobNotEnqueueableError(job.name);
		}

		return this.bullMqRegistry.getOrCreateQueue(job.queueName);
	}

	/** Returns a native worker owned by this factory, after application initialization. */
	public getWorker(workerId: symbol) {
		return this.bullMqRegistry.getWorker(workerId);
	}

	public add<
		TJobInstance extends InstanceType<TEnqueueable[keyof TEnqueueable]>,
	>(
		job: TJobInstance,
		options?: JobsOptions,
	): Promise<Job<TJobInstance["payload"], unknown, string>> {
		const jobClass = job.constructor as TEnqueueable[keyof TEnqueueable];

		return this.getQueue(jobClass).add(jobClass.jobName, job.payload, {
			...jobClass.defaultJobOptions,
			...options,
		});
	}
}
