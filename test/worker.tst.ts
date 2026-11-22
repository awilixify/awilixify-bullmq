import { Type } from "@sinclair/typebox";
import type { Job, JobProgress } from "bullmq";
import { describe, expect, test } from "tstyche";
import { defineJob, type JobOf, onWorkerEvent, processJob } from "../index.js";
import { jobWithSchema } from "../lib/typebox.js";

class Transcode extends defineJob<{ source: string }>()({
	queueName: "audio",
	jobName: "transcode",
}) {}
class NormalizeAudio extends defineJob<{ gain: number }>()({
	queueName: "audio",
	jobName: "normalize",
}) {}
class OtherTranscode extends defineJob<{ url: string }>()({
	queueName: "audio",
	jobName: "transcode",
}) {}
class ProcessOrder extends jobWithSchema(Type.Object({ id: Type.Number() }), {
	queueName: "orders",
	jobName: "process-order",
}) {}

declare function methodContextFor<TThis>(): <
	Method extends (...args: any[]) => unknown,
>(
	method: Method,
) => ClassMethodDecoratorContext<TThis, Method>;

describe("processJob", () => {
	test("rejects duplicate literal job names within a processor tuple", () => {
		expect(processJob).type.not.toBeCallableWith([Transcode, OtherTranscode]);
		expect(processJob).type.not.toBeCallableWith([
			Transcode,
			NormalizeAudio,
			Transcode,
		]);
	});

	test("rejects job instances in place of constructors", () => {
		expect(processJob).type.not.toBeCallableWith(
			new Transcode({ source: "file.wav" }),
		);
	});

	test("rejects factory-owned processor options", () => {
		expect(processJob).type.not.toBeCallableWith(Transcode, {
			connection: {},
		});
		expect(processJob).type.not.toBeCallableWith(Transcode, {
			prefix: "wrong",
		});
	});

	test("checks processor payloads and decorated method compatibility", () => {
		class Controller {
			@processJob([Transcode, NormalizeAudio])
			multiple(job: JobOf<typeof Transcode | typeof NormalizeAudio>) {
				expect(job.data).type.toBe<{ source: string } | { gain: number }>();
				switch (job.name) {
					case "transcode":
						expect(job.data).type.toBe<{ source: string }>();
						break;
					case "normalize":
						expect(job.data).type.toBe<{ gain: number }>();
						break;
					default:
						expect(job).type.toBe<never>();
				}
			}

			@processJob(ProcessOrder)
			order(job: JobOf<typeof ProcessOrder>) {
				expect(job.data).type.toBe<{ id: number }>();
				expect(job.name).type.toBe<"process-order">();
			}

			missingPayload(_job: Job<Transcode["payload"]>) {}

			wrongPayload(_job: Job<{ source: number }>) {}

			payloadOnly(_payload: Transcode["payload"]) {}
		}

		const methodContext = methodContextFor<Controller>();

		expect(processJob).type.not.toBeCallableWith([]);
		expect(processJob).type.toBeCallableWith([Transcode]);

		expect(processJob([Transcode, NormalizeAudio])).type.not.toBeCallableWith(
			Controller.prototype.missingPayload,
			methodContext(Controller.prototype.missingPayload),
		);
		expect(processJob(Transcode)).type.not.toBeCallableWith(
			Controller.prototype.wrongPayload,
			methodContext(Controller.prototype.wrongPayload),
		);
		expect(processJob(Transcode)).type.not.toBeCallableWith(
			Controller.prototype.payloadOnly,
			methodContext(Controller.prototype.payloadOnly),
		);
	});
});

describe("onWorkerEvent", () => {
	type AudioJob = JobOf<typeof Transcode | typeof NormalizeAudio>;

	class Consumer {
		@processJob([Transcode, NormalizeAudio])
		async process(_job: AudioJob) {
			return 42;
		}

		@onWorkerEvent("process", "active")
		active(_job: AudioJob, _prev: string) {}

		@onWorkerEvent("process", "completed")
		completed(_job: AudioJob, _result: number, _prev: string) {}

		@onWorkerEvent("process", "failed")
		failed(_job: AudioJob | undefined, _error: Error, _prev: string) {}

		@onWorkerEvent("process", "progress")
		progress(_job: AudioJob, _progress: JobProgress) {}

		@onWorkerEvent("process", "error")
		error(_error: Error) {}

		@onWorkerEvent("process", "ready")
		ready() {}

		missing(_job: AudioJob) {}

		value = 123;
		notMethod() {}

		wrongPayload(_job: Job<{ unrelated: boolean }>) {}

		incompleteUnion(_job: JobOf<typeof Transcode>) {}

		wrongPrev(_job: AudioJob, _prev: number) {}

		wrongResult(_job: AudioJob, _result: string) {}

		missingUndefined(_job: AudioJob, _error: Error) {}

		wrongError(_error: string) {}
	}

	const methodContext = methodContextFor<Consumer>();

	test("checks decorated method compatibility", () => {
		expect(onWorkerEvent).type.not.toBeCallableWith("process", "unknown-event");
		expect(onWorkerEvent("renamedProcess", "active")).type.not.toBeCallableWith(
			Consumer.prototype.missing,
			methodContext(Consumer.prototype.missing),
		);
		expect(onWorkerEvent("value", "ready")).type.not.toBeCallableWith(
			Consumer.prototype.notMethod,
			methodContext(Consumer.prototype.notMethod),
		);
		expect(onWorkerEvent("process", "active")).type.not.toBeCallableWith(
			Consumer.prototype.wrongPayload,
			methodContext(Consumer.prototype.wrongPayload),
		);
		expect(onWorkerEvent("process", "active")).type.not.toBeCallableWith(
			Consumer.prototype.incompleteUnion,
			methodContext(Consumer.prototype.incompleteUnion),
		);
		expect(onWorkerEvent("process", "active")).type.not.toBeCallableWith(
			Consumer.prototype.wrongPrev,
			methodContext(Consumer.prototype.wrongPrev),
		);
		expect(onWorkerEvent("process", "completed")).type.not.toBeCallableWith(
			Consumer.prototype.wrongResult,
			methodContext(Consumer.prototype.wrongResult),
		);
		expect(onWorkerEvent("process", "failed")).type.not.toBeCallableWith(
			Consumer.prototype.missingUndefined,
			methodContext(Consumer.prototype.missingUndefined),
		);
		expect(onWorkerEvent("process", "error")).type.not.toBeCallableWith(
			Consumer.prototype.wrongError,
			methodContext(Consumer.prototype.wrongError),
		);
	});
});
