import { Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";
import { UnrecoverableError } from "bullmq";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createQueueModuleFactory,
	defineJob,
	type JobOf,
	processJob,
} from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
import { jobWithSchema, type SchemaJobConstructor } from "../lib/typebox.js";
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

describe("consumer validation", () => {
	class ValidatedEmail extends jobWithSchema(
		Type.Object({ to: Type.String() }),
		{
			queueName: "email",
			jobName: "send-email",
		},
	) {}
	class ValidatedDigest extends jobWithSchema(
		Type.Object({ recipients: Type.Array(Type.String()) }),
		{
			queueName: "email",
			jobName: "send-digest",
		},
	) {}

	function setupValidation(
		validate: (JobType: SchemaJobConstructor, payload: unknown) => undefined,
		jobs: readonly [SchemaJobConstructor, ...SchemaJobConstructor[]] = [
			ValidatedEmail,
			ValidatedDigest,
		],
	) {
		const ValidatedQueueModule = createQueueModuleFactory({
			connection: {},
			validate,
		});
		const work = vi.fn();
		class Controller {
			@processJob(jobs)
			execute(job: JobOf<SchemaJobConstructor>) {
				return work(job);
			}
		}
		const app = createApp({
			imports: [
				ValidatedQueueModule({
					enqueueable: Object.fromEntries(
						jobs.map((JobType) => [JobType.name, JobType]),
					),
					processable: jobs,
				}),
			],
			controllers: [Controller],
		});

		return { app, work };
	}

	it("allows plain and schema-bearing jobs in the same validating processor", async () => {
		class SendEmail extends defineJob<{ to: string }>()({
			queueName: "email",
			jobName: "send-email",
		}) {}
		class SchemaEmail extends jobWithSchema(
			Type.Object({ to: Type.String() }),
			{
				queueName: "email",
				jobName: "schema-email",
			},
		) {}
		const error = new UnrecoverableError("Invalid payload");
		const { app, work } = setupValidation(
			(JobType, payload) => {
				if (JobType.schema && !Value.Check(JobType.schema, payload))
					throw error;
			},
			[SendEmail, SchemaEmail],
		);

		await app.init();

		const processor = mocks.workers[0].processor;
		const plainJob = { name: "send-email", data: { to: "plain" } };
		const schemaJob = { name: "schema-email", data: { to: "validated" } };
		await processor(plainJob);
		expect(work).toHaveBeenLastCalledWith(plainJob);
		await processor(schemaJob);
		expect(work).toHaveBeenLastCalledWith(schemaJob);
		await expect(
			processor({ name: "schema-email", data: { to: 123 } }),
		).rejects.toBe(error);
		expect(work).toHaveBeenCalledTimes(2);
	});

	it("validates the matching schema before consuming, not during enqueueing", async () => {
		const calls: string[] = [];
		const validate = vi.fn(
			(_JobType: SchemaJobConstructor, _payload: unknown): undefined => {
				calls.push("validate");
			},
		);
		const { app, work } = setupValidation(validate);
		work.mockImplementation(() => {
			calls.push("consume");
		});

		await app.init();

		await app.scope
			.resolve<BullMqService<{ ValidatedEmail: typeof ValidatedEmail }>>(
				"bullMq",
			)
			.add(new ValidatedEmail({ to: "user" }));
		expect(validate).not.toHaveBeenCalled();

		const processor = mocks.workers[0].processor;
		const email = { name: "send-email", data: { to: "user" } };
		const digest = { name: "send-digest", data: { recipients: ["user"] } };
		const processing = processor(email);
		expect(calls[0]).toBe("validate");
		await processing;
		expect(validate).toHaveBeenLastCalledWith(ValidatedEmail, email.data);
		expect(work).toHaveBeenLastCalledWith(email);
		await processor(digest);
		expect(validate).toHaveBeenLastCalledWith(ValidatedDigest, digest.data);
		expect(work).toHaveBeenLastCalledWith(digest);
		expect(calls).toEqual(["validate", "consume", "validate", "consume"]);
	});

	it("rejects unknown names without skipping validation", async () => {
		const validate = vi.fn(
			(_JobType: SchemaJobConstructor, _payload: unknown) => undefined,
		);
		const { app, work } = setupValidation(validate);

		await app.init();

		await expect(
			mocks.workers[0].processor({ name: "external-job", data: {} }),
		).rejects.toBeInstanceOf(UnrecoverableError);
		expect(validate).not.toHaveBeenCalled();
		expect(work).not.toHaveBeenCalled();
	});
});
