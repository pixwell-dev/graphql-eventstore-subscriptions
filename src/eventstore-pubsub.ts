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
  username?: string;
  password?: string;
  client?: EventStoreNodeConnection;
}

export class EventStorePubSub implements PubSubEngine {
  private eventStoreConnection: EventStoreNodeConnection;
  private subscriptions: Map<number, EventStoreSubscription> = new Map();
  private nextSubscriptionId: number = 0;
  private retryAttempts: number = 0;

  constructor(options: PubSubEventStoreOptions = {}) {
    this.connect(options);
  }

  public connect(options: PubSubEventStoreOptions = {}) {
    this.eventStoreConnection = createConnection({
      defaultUserCredentials: {
        username: options.username,
        password: options.password,
      },
    }, {
      host: options.host,
      port: options.port,
    } as unknown as TcpEndPoint);
    this.eventStoreConnection.connect();

    this.eventStoreConnection.on('connected', () => {
      // console.info('Connection to EventStore established!');
      this.retryAttempts = 0;
    });

    this.eventStoreConnection.on('closed', () => {
      this.retryAttempts += 1;
      console.error(`Connection to EventStore closed! reconnecting attempt(${this.retryAttempts})...`);
      this.connect(options);
    });
    this.eventStoreConnection.on('error', console.error);
  }

  // @ts-ignore
  public async publish(triggerName: string, payload: any): Promise<void> {
    // noop
  }

  // @ts-ignore
  public async subscribe(triggerName: string, onMessage: Function, options?: Object): Promise<number> {
    const subscriptionId = this.getNextSubscriptionId();
    try {
      const result = await this.eventStoreConnection.subscribeToStream(
        triggerName,
        true,
        (_, payload) => onMessage(payload),
        (sub, reason, error) =>
          this.onDropped(sub, reason, error, onMessage, options)
      );

      this.subscriptions.set(subscriptionId, result);

      // console.info('Volatile processing of EventStore events started! Stream: ' + triggerName);
      return Promise.resolve(subscriptionId);
    } catch (err) {
      console.error(err);
      this.reSubscribeToSubscription(triggerName, onMessage, options);
    }
  }

  public onDropped(subscription, _reason: string, error: Error, onMessage: Function, options?: Object) {
    subscription.isLive = false;
    subscription.unsubscribe();
    subscription.close();
    console.error('onDropped => ' + error, error.stack);
    this.reSubscribeToSubscription((subscription as any).streamId, onMessage, options);
  }

  public reSubscribeToSubscription(stream: string, onMessage: Function, options?: Object) {
    console.warn(`connecting to subscription ${stream}. Retrying...`);
    setTimeout(
      (stream) => this.subscribe(stream, onMessage, options),
      3000,
      stream,
    );
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
