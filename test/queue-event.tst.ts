import type { Job } from "bullmq";
import { expect, test } from "tstyche";
import { defineJob, onQueueEvent, type QueueEventOf } from "../index.js";

class Transcode extends defineJob<{ source: string }>()({
	queueName: "audio",
	jobName: "transcode",
}) {}
declare function methodContext<Method extends (...args: any[]) => unknown>(
	method: Method,
): ClassMethodDecoratorContext<Observer, Method>;

class Observer {
	@onQueueEvent("external-audio", "completed")
	completed(event: QueueEventOf<"completed">, _id: string) {
		expect(event.returnvalue).type.toBe<unknown>();
		expect(event.jobId).type.toBe<string>();
	}

	@onQueueEvent(Transcode.queueName, "failed")
	failed(event: QueueEventOf<"failed">) {
		expect(event.failedReason).type.toBe<string>();
	}

	@onQueueEvent("audio", "drained")
	drained(_id: string) {}

	@onQueueEvent("audio", "error")
	error(_error: Error) {}

	@onQueueEvent("audio", "ioredis:close")
	closed() {}

	wrongData(_event: { jobId: number }) {}

	wrongResult(_event: { jobId: string; returnvalue: string }) {}

	wrongStreamId(_event: QueueEventOf<"completed">, _id: number) {}

	notAJob(_event: Job) {}

	wrongError(_error: string) {}
}

test("checks decorated method compatibility", () => {
	expect(onQueueEvent).type.not.toBeCallableWith(Transcode, "completed");
	expect(onQueueEvent).type.not.toBeCallableWith("audio", "unknown-event");

	expect(onQueueEvent("audio", "completed")).type.not.toBeCallableWith(
		Observer.prototype.wrongData,
		methodContext(Observer.prototype.wrongData),
	);
	expect(onQueueEvent("audio", "completed")).type.not.toBeCallableWith(
		Observer.prototype.wrongResult,
		methodContext(Observer.prototype.wrongResult),
	);
	expect(onQueueEvent("audio", "completed")).type.not.toBeCallableWith(
		Observer.prototype.wrongStreamId,
		methodContext(Observer.prototype.wrongStreamId),
	);
	expect(onQueueEvent("audio", "completed")).type.not.toBeCallableWith(
		Observer.prototype.notAJob,
		methodContext(Observer.prototype.notAJob),
	);
	expect(onQueueEvent("audio", "error")).type.not.toBeCallableWith(
		Observer.prototype.wrongError,
		methodContext(Observer.prototype.wrongError),
	);
});
