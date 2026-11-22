import type { InitializerContext } from "awilixify";
import { Initializer } from "awilixify";
import type { ConnectionOptions } from "bullmq";
import * as ERRORS from "./errors.js";
import type { Deps } from "./queue.module.js";
import type { JobConstructor } from "./worker.decorator.js";
import { workerState } from "./worker.decorator.js";

type QueueJobToken = typeof workerState.token;

export class WorkerInitializer<
	TConnection = ConnectionOptions,
> extends Initializer<QueueJobToken> {
	public readonly token = workerState.token;

	constructor(
		private readonly bullMqRegistry: Deps<TConnection>["bullMqRegistry"],
		private readonly processableJobs: Deps<TConnection>["processableJobs"],
		private readonly bullMqOptions: Deps<TConnection>["bullMqOptions"],
	) {
		super();
	}

	initialize(context: InitializerContext<QueueJobToken>): void {
		const { jobs, workerEvents } = context.metadata;

		if (jobs !== undefined && workerEvents.length) {
			throw new ERRORS.ConflictingWorkerMethodRolesError(context.methodName);
		}

		if (jobs === undefined) {
			this.registerWorkerEvents(context);

			return;
		}

		const first = jobs[0];

		if (!first) throw new ERRORS.EmptyProcessorJobsError();

		const queueName = first.queueName;
		const jobsByName = new Map<string, JobConstructor>();

		for (const jobClass of jobs) {
			const registeredJob = this.processableJobs.find(
				(candidate) => candidate === jobClass,
			);

			if (!registeredJob) {
				throw new ERRORS.JobNotProcessableError(
					jobClass.name,
					context.moduleName,
				);
			}

			if (jobsByName.has(jobClass.jobName)) {
				throw new ERRORS.DuplicateJobNameError(jobClass.jobName, queueName);
			}

			jobsByName.set(jobClass.jobName, registeredJob);

			if (jobClass.queueName !== queueName) {
				throw new ERRORS.ProcessorQueueMismatchError(
					queueName,
					jobClass.queueName,
					jobClass.name,
				);
			}
		}

		const { workerId, ...workerOptions } = context.metadata.workerOptions ?? {};

		this.bullMqRegistry.registerProcessor({
			target: context.target,
			methodName: context.methodName,
			queueName,
			workerId,
			workerOptions,
			processor: async (job) => {
				if (this.bullMqOptions.validate) {
					const JobType = jobsByName.get(job.name);

					if (!JobType) {
						throw new ERRORS.UnknownJobNameError(job.name, queueName);
					}

					this.bullMqOptions.validate(JobType, job.data);
				}

				return context.invoke(job);
			},
		});
	}

	private registerWorkerEvents(
		context: InitializerContext<QueueJobToken>,
	): void {
		for (const registration of context.metadata.workerEvents) {
			const processor = context.decoratorState.methods.get(
				registration.processorMethod,
			);

			if (processor?.jobs === undefined) {
				throw new ERRORS.InvalidWorkerEventTargetError(
					context.methodName,
					registration.processorMethod,
					context.target.name,
				);
			}

			this.bullMqRegistry.registerWorkerEventListener({
				target: context.target,
				...registration,
				invoke: context.invoke,
			});
		}
	}
}
