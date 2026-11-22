import type { Static, TSchema } from "@sinclair/typebox";
import {
	defineJob,
	type JobConstructor,
	type JobDefinition,
} from "./worker.decorator.js";

export type SchemaJobConstructor = JobConstructor & {
	readonly schema?: TSchema;
};

export function jobWithSchema<
	TSchemaType extends TSchema,
	const TDefinition extends JobDefinition,
>(schema: TSchemaType, definition: TDefinition) {
	return class extends defineJob<Static<TSchemaType>>()(definition) {
		static readonly schema = schema;
	};
}
