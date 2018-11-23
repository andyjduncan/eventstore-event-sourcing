'use strict';

const AWS = require('aws-sdk');

const {fetchSubscriptionEvent, ackSubscription, populateEntity, publishEvent} = require('./eventstore');

class ApiMessage {

    constructor(streamUri) {
        this.streamUri = streamUri;

        this.s3 = new AWS.S3();

        this.assets = {};
    }

    async initialise() {
        await populateEntity(this);
    }

    async addS3Asset(bucket, prefix, location) {
        await publishEvent(this.streamUri, 'S3AssetAdded', {
            bucket,
            key: `${prefix}/${location}`
        });
    }

    applyS3AssetAdded(event) {
        this.assets.push(event.key);
    }

    async assetProcessingFailed(key) {
        await publishEvent(this.streamUri, 'S3AssetProcessingFailed', {key})
    }

    async assetProcessingSucceeded(key) {
        await publishEvent(this.streamUri, 'S3AssetProcessingSucceeded', {key})
    }

    applyS3AssetProcessingFailed(event) {

    }

    applyS3AssetProcessingSucceeded(event) {

    }
}

const processNextMessage = async () => {
    const eventStore = process.env.EVENT_STORE_ROOT;

    const uri = `${eventStore}/subscriptions/$et-MessageReceived/api-processor`;

    const subscriptionEvent = await fetchSubscriptionEvent(uri);

    if (!subscriptionEvent) {
        return false;
    }

    const event = subscriptionEvent.event.data;

    const {bucket, prefix} = subscriptionEvent.event.metadata.location.s3;

    const messageId = `${event.sender}-${event.messageId}`;

    const apiMessageStream = `${eventStore}/streams/apimessage-${messageId}`;

    const apiMessage = new ApiMessage(apiMessageStream);

    await apiMessage.initialise();

    for (let asset of event.assets) {
        await apiMessage.addS3Asset(bucket, prefix, asset.location);
    }

    await ackSubscription(subscriptionEvent);

    return true;
};

const processNextAsset = async () => {
    const eventStore = process.env.EVENT_STORE_ROOT;

    const uri = `${eventStore}/subscriptions/$et-S3AssetAdded/s3-asset-processor`;

    const subscriptionEvent = await fetchSubscriptionEvent(uri);

    if (!subscriptionEvent) {
        return false;
    }

    const event = subscriptionEvent.event;

    const apiMessageStream = `${eventStore}/streams/apimessage-${event.eventStreamId}`;

    const apiMessage = new ApiMessage(apiMessageStream);

    await apiMessage.initialise();

    if (Math.random() > 0.9) {
        await apiMessage.assetProcessingFailed(event.data.key);
    } else {
        await apiMessage.assetProcessingSucceeded(event.data.key);
    }

    await ackSubscription(subscriptionEvent);

    return true;
};

module.exports.processMessages = async (event, context) => {
    const timeToProcess = 1000;

    let messageProcessed = true;

    while (context.getRemainingTimeInMillis() > timeToProcess && messageProcessed) {
        messageProcessed = await processNextMessage();
    }

    console.log('Done');
};

module.exports.processAssets = async (event, context) => {
    const timeToProcess = 1000;

    let messageProcessed = true;

    while (context.getRemainingTimeInMillis() > timeToProcess && messageProcessed) {
        messageProcessed = await processNextAsset();
    }

    console.log('Done');
};