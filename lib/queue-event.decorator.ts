import { createDecoratorStateUpdater } from "awilixify";
import type { QueueEventsListener } from "bullmq";

export type QueueEvent = keyof QueueEventsListener;
export type QueueEventArgs<TEvent extends QueueEvent> = Parameters<
	QueueEventsListener<unknown>[TEvent]
>;
export type QueueEventOf<TEvent extends QueueEvent> = QueueEventArgs<TEvent>[0];

type QueueEventRegistration = {
	queueName: string;
	event: QueueEvent;
};

export const queueEventState: ReturnType<
	typeof createDecoratorStateUpdater<{
		method: () => readonly QueueEventRegistration[];
	}>
> = createDecoratorStateUpdater("BullMQ Queue Events", {
	method: (): readonly QueueEventRegistration[] => [],
});

export function onQueueEvent<const TEvent extends QueueEvent>(
	queueName: string,
	event: TEvent,
) {
	return <TThis, TReturn>(
		_target: (this: TThis, ...args: QueueEventArgs<TEvent>) => TReturn,
		context: ClassMethodDecoratorContext<
			TThis,
			(this: TThis, ...args: QueueEventArgs<TEvent>) => TReturn
		>,
	) => {
		queueEventState.update(context, {
			method: (registrations) => [...registrations, { queueName, event }],
		});
	};
}
