import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub-engine';
import {
  createConnection,
  EventStoreNodeConnection,
  EventStoreSubscription,
  TcpEndPoint,
} from 'node-eventstore-client';
import { PubSubAsyncIterator } from './pubsub-async-iterator';

export interface PubSubEventStoreOptions {
  host?: string;
  port?: string;
  client?: EventStoreNodeConnection;
}

export class EventStorePubSub implements PubSubEngine {
  private eventStoreConnection: EventStoreNodeConnection;
  private subscriptions: Map<number, EventStoreSubscription> = new Map();
  private nextSubscriptionId: number = 0;

  constructor(options: PubSubEventStoreOptions = {}) {
    this.eventStoreConnection = createConnection({}, {
      host: options.host,
      port: options.port,
    } as unknown as TcpEndPoint);
    this.eventStoreConnection.connect();
    this.eventStoreConnection.on('error', console.error);
  }

  public async publish(triggerName: string, payload: any): Promise<void> {
    // noop
  }

  public async subscribe(triggerName: string, onMessage: Function, options?: Object): Promise<number> {
    const subscriptionId = this.getNextSubscriptionId();
    try {
      const result = await this.eventStoreConnection.subscribeToStream(
        triggerName,
        true,
        (sub, payload) => onMessage(sub, payload),
      );

      this.subscriptions.set(subscriptionId, result);

      return Promise.resolve(subscriptionId);
    } catch (err) {
      console.error(err);
    }
  }

  public unsubscribe(subId: number) {
    const subs = this.subscriptions.get(subId);

    if (!subs) {
      return;
    }

    subs.unsubscribe();
    this.subscriptions.delete(subId);
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers);
  }

  private getNextSubscriptionId(): number {
    this.nextSubscriptionId += 1;
    return this.nextSubscriptionId;
  }
}
