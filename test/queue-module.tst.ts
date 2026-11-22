import { type TSchema, Type } from "@sinclair/typebox";
import type { ModuleDef } from "awilixify";
import {
	createPostgresBackend,
	type Job,
	type PostgresConnectionOptions,
} from "bullmq";
import { describe, expect, test } from "tstyche";
import { createQueueModuleFactory, defineJob } from "../index.js";
import type { BullMqService } from "../lib/bullmq.service.js";
import { jobWithSchema, type SchemaJobConstructor } from "../lib/typebox.js";

class SendEmail extends defineJob<{ to: string }>()({
	queueName: "email",
	jobName: "send-email",
}) {}
class ResizeImage extends defineJob<{ url: string }>()({
	queueName: "image",
	jobName: "resize-image",
}) {}

const QueueModule = createQueueModuleFactory({
	connection: { host: "localhost" },
});
const module = QueueModule({
	enqueueable: { SendEmail },
	processable: [ResizeImage],
});
type Def = ModuleDef<{ imports: [typeof module] }>;
declare const queue: Def["deps"]["bullMq"];

const mixed = QueueModule({
	enqueueable: { SendEmail, ResizeImage },
	processable: [],
});
type MixedDef = ModuleDef<{ imports: [typeof mixed] }>;
declare const mixedQueue: MixedDef["deps"]["bullMq"];

describe("module scope", () => {
	const empty = QueueModule({ enqueueable: {}, processable: [] });
	type EmptyDef = ModuleDef<{ imports: [typeof empty] }>;
	let emptyQueue!: EmptyDef["deps"]["bullMq"];

	test("limits native queue access to enqueueable constructors", () => {
		expect(queue.getQueue).type.toBeCallableWith(SendEmail);
		expect(queue.getQueue).type.not.toBeCallableWith(ResizeImage);
		expect(queue.getQueue).type.not.toBeCallableWith(
			new SendEmail({ to: "user" }),
		);
		expect(emptyQueue.getQueue).type.not.toBeCallableWith(SendEmail);
		expect(mixedQueue.getQueue).type.toBeCallableWith(ResizeImage);
	});

	test("accepts only enqueueable jobs", () => {
		expect(queue.add).type.toBeCallableWith(new SendEmail({ to: "user" }));
		expect(queue.add).type.not.toBeCallableWith(
			new ResizeImage({ url: "image.png" }),
		);
		expect(emptyQueue.add).type.not.toBeCallableWith(
			new SendEmail({ to: "user" }),
		);
	});
});

describe("payload inference", () => {
	test("preserves registered constructors and enqueue payload types", () => {
		expect(queue).type.toBe<
			BullMqService<{ readonly SendEmail: typeof SendEmail }>
		>();
		expect(queue.jobs.SendEmail).type.toBe<typeof SendEmail>();
		expect(SendEmail).type.not.toBeConstructableWith({ to: 42 });
		expect(mixedQueue.add(new SendEmail({ to: "user" }))).type.toBe<
			Promise<Job<{ to: string }, unknown, string>>
		>();
	});

	test("preserves schema-derived types through registration", () => {
		const schema = Type.Object({ id: Type.Number() });
		class ProcessOrder extends jobWithSchema(schema, {
			queueName: "orders",
			jobName: "process-order",
		}) {}
		const schemaModule = QueueModule({
			enqueueable: { ProcessOrder },
			processable: [],
		});
		type SchemaDef = ModuleDef<{ imports: [typeof schemaModule] }>;
		let schemaQueue!: SchemaDef["deps"]["bullMq"];

		expect(ProcessOrder).type.not.toBeConstructableWith({ id: "1" });
		expect(schemaQueue.jobs.ProcessOrder.schema).type.toBe<typeof schema>();
		expect(schemaQueue.add(new ProcessOrder({ id: 1 }))).type.toBe<
			Promise<Job<{ id: number }, unknown, string>>
		>();
	});
});

describe("factory configuration", () => {
	test("preserves backend connection types through native access", () => {
		const PostgresQueueModule = createQueueModuleFactory({
			backendFactory: createPostgresBackend,
			connection: { database: "jobs" },
		});
		const postgresModule = PostgresQueueModule({
			enqueueable: { SendEmail },
			processable: [],
		});
		type PostgresDef = ModuleDef<{ imports: [typeof postgresModule] }>;
		let postgresQueue!: PostgresDef["deps"]["bullMq"];

		expect(
			postgresQueue.getQueue(SendEmail).opts.connection,
		).type.toBe<PostgresConnectionOptions>();
		expect(
			postgresQueue.getWorker(Symbol("worker")).opts.connection,
		).type.toBe<PostgresConnectionOptions>();
	});

	test("rejects incompatible validators", () => {
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			validate: () => new Error("invalid"),
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			validate: async () => undefined,
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			validate: (
				_JobType: SchemaJobConstructor & { readonly schema: TSchema },
				_payload: unknown,
			) => {},
		});
	});

	test("accepts prefix only at the factory level", () => {
		createQueueModuleFactory({ connection: {}, prefix: "my-app" });
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			queueOptions: { prefix: "wrong" },
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			workerOptions: { prefix: "wrong" },
		});
	});

	test("rejects factory-owned queue-event options", () => {
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			queueEventsOptions: { prefix: "wrong" },
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: {},
			queueEventsOptions: { connection: {} },
		});
	});

	test("infers connection options from the backend factory", () => {
		createQueueModuleFactory({
			backendFactory: createPostgresBackend,
			connection: "postgres://localhost/jobs",
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			backendFactory: createPostgresBackend,
			connection: { migrate: "yes" },
		});
		expect(createQueueModuleFactory).type.not.toBeCallableWith({
			connection: "postgres://localhost/jobs",
		});
	});
});
