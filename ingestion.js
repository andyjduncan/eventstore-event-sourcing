'use strict';

const AWS = require('aws-sdk');

const memoize = require('fast-memoize');

const {Base64} = require('js-base64');

const fetch = require('node-fetch');

const uuid = require('uuid/v4');

module.exports.trigger = async (event) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = event.Records[0].s3.object.key;

    const S3 = new AWS.S3();

    const s3Object = await S3.getObject({
        Bucket: bucket,
        Key: key
    }).promise();

    const message = JSON.parse(s3Object.Body.toString());

    const metadata = {
        location: {
            s3: {
                bucket: bucket,
                prefix: key.split('/').slice(0, -1).join('/'),
                key: key
            }
        }
    };

    const messageBody = [
        {
            eventId: uuid(),
            eventType: 'MessageReceived',
            data: message,
            metadata: metadata
        }
    ];

    const headers = {
        'Content-Type': 'application/vnd.eventstore.events+json',
        'Accept': 'application/json',
        'Authorization': (await authentication()),
        'ES-ExpectedVersion': '-1'
    };

    const eventStore = process.env.EVENT_STORE_ROOT;

    const uri = `/streams/message-${message.sender}-${message.messageId}`;

    const eventResponse = await fetch(`${eventStore}${uri}`, {
        method: 'POST',
        body: JSON.stringify(messageBody),
        headers: headers
    });

    console.log(`${eventResponse.ok} - ${eventResponse.status} ${eventResponse.statusText}`);
};

const authentication = memoize(async () => {
    const ssm = new AWS.SSM();

    const parameters = await ssm.getParameters({
        Names: ['/eventstore/ingestion/username', '/eventstore/ingestion/password'],
        WithDecryption: true
    }).promise();

    const username = parameters.Parameters.find((p) => p.Name === '/eventstore/ingestion/username').Value;
    const password = parameters.Parameters.find((p) => p.Name === '/eventstore/ingestion/password').Value;

    const auth = Base64.encode(`${username}:${password}`);

    return `Basic ${auth}`;
});
