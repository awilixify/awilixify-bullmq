import { UnrecoverableError } from "bullmq";

export class RegistryShutdownError extends Error {
	constructor(
		operation:
			| "access queues"
			| "access workers"
			| "register processors"
			| "register worker events"
			| "register queue events"
			| "register workers",
	) {
		super(`Cannot ${operation} after shutdown`);
		this.name = "RegistryShutdownError";
	}
}

export class InvalidWorkerIdError extends Error {
	constructor() {
		super("Worker IDs must be symbols");
		this.name = "InvalidWorkerIdError";
	}
}

export class DuplicateWorkerIdError extends Error {
	constructor(workerId: symbol) {
		super(
			`Worker ID "${String(workerId)}" is already registered in this factory`,
		);
		this.name = "DuplicateWorkerIdError";
	}
}

export class WorkerNotAvailableError extends Error {
	constructor(workerId: symbol) {
		super(
			`Worker "${String(workerId)}" is not registered or not initialized in this factory; access workers after application initialization`,
		);
		this.name = "WorkerNotAvailableError";
	}
}

export class BullMqShutdownError extends AggregateError {
	constructor(errors: Iterable<unknown>) {
		super(errors, "BullMQ shutdown failed");
		this.name = "BullMqShutdownError";
	}
}

export class ConflictingWorkerMethodRolesError extends Error {
	constructor(methodName: string | symbol) {
		super(
			`Method "${String(methodName)}" cannot be both a processor and a worker event handler`,
		);
		this.name = "ConflictingWorkerMethodRolesError";
	}
}

export class EmptyProcessorJobsError extends Error {
	constructor() {
		super("A processor must declare at least one job");
		this.name = "EmptyProcessorJobsError";
	}
}

export class JobNotProcessableError extends Error {
	constructor(jobClassName: string, moduleName: string) {
		super(`Job "${jobClassName}" is not processable in module "${moduleName}"`);
		this.name = "JobNotProcessableError";
	}
}

export class DuplicateJobNameError extends Error {
	constructor(jobName: string, queueName: string) {
		super(`Duplicate job name "${jobName}" in queue "${queueName}"`);
		this.name = "DuplicateJobNameError";
	}
}

export class ProcessorQueueMismatchError extends Error {
	constructor(
		expectedQueueName: string,
		queueName: string,
		jobClassName: string,
	) {
		super(
			`Jobs in one processor must share a queue: expected "${expectedQueueName}", received "${queueName}" for job "${jobClassName}"`,
		);
		this.name = "ProcessorQueueMismatchError";
	}
}

export class UnknownJobNameError extends UnrecoverableError {
	constructor(jobName: string, queueName: string) {
		super(
			`Unknown job name "${jobName}" for processor on queue "${queueName}"`,
		);
	}
}

export class InvalidWorkerEventTargetError extends Error {
	constructor(
		handlerMethod: string | symbol,
		processorMethod: string | symbol,
		controllerName: string,
	) {
		super(
			`Worker event handler "${String(handlerMethod)}" targets "${String(processorMethod)}", which is not a @processJob method on "${controllerName}"`,
		);
		this.name = "InvalidWorkerEventTargetError";
	}
}

export class JobNotEnqueueableError extends Error {
	constructor(jobClassName: string) {
		super(`Job "${jobClassName}" is not enqueueable in this module scope`);
		this.name = "JobNotEnqueueableError";
	}
}

export class MultipleProcessJobDecoratorsError extends Error {
	constructor(methodName: string | symbol) {
		super(
			`Method "${String(methodName)}" cannot have multiple @processJob decorators; use one decorator with a job array`,
		);
		this.name = "MultipleProcessJobDecoratorsError";
	}
}
