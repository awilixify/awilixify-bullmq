import { Initializer, type InitializerContext } from "awilixify";
import type { ConnectionOptions } from "bullmq";
import type { Deps } from "./queue.module.js";
import { queueEventState } from "./queue-event.decorator.js";

type QueueEventToken = typeof queueEventState.token;

export class QueueEventInitializer<
	TConnection = ConnectionOptions,
> extends Initializer<QueueEventToken> {
	public readonly token = queueEventState.token;

	constructor(
		private readonly bullMqRegistry: Deps<TConnection>["bullMqRegistry"],
	) {
		super();
	}

	initialize(context: InitializerContext<QueueEventToken>): void {
		for (const registration of context.metadata) {
			this.bullMqRegistry.registerQueueEventListener({
				...registration,
				invoke: context.invoke,
			});
		}
	}
}
