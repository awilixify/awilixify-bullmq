import { Type } from "@sinclair/typebox";
import { describe, expect, it } from "vitest";
import { defineJob } from "../index.js";

import { jobWithSchema } from "../lib/typebox.js";

describe("job definitions", () => {
	it("inherits static metadata and constructs typed payload instances", () => {
		const defaultJobOptions = { attempts: 3 };
		class Transcode extends defineJob<{ source: string }>()({
			queueName: "audio",
			jobName: "transcode",
			defaultJobOptions,
		}) {}

		const payload = { source: "file.wav" };
		const job = new Transcode(payload);

		expect(job).toBeInstanceOf(Transcode);
		expect(job.payload).toBe(payload);
		expect(Transcode.queueName).toBe("audio");
		expect(Transcode.jobName).toBe("transcode");
		expect(Transcode.defaultJobOptions).toBe(defaultJobOptions);
	});

	it("attaches the original schema without validating or transforming payloads", () => {
		const schema = Type.Object({ id: Type.Number() });
		class ProcessOrder extends jobWithSchema(schema, {
			queueName: "orders",
			jobName: "process-order",
		}) {}

		expect(ProcessOrder.schema).toBe(schema);
		expect(ProcessOrder.queueName).toBe("orders");
		expect(ProcessOrder.jobName).toBe("process-order");
		expect(new ProcessOrder({ id: 1 }).payload).toEqual({ id: 1 });
		expect(new ProcessOrder({ id: "unvalidated" } as any).payload).toEqual({
			id: "unvalidated",
		});
	});
});
