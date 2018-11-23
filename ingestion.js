'use strict';

const AWS = require('aws-sdk');

const {publishEvent} = require('./eventstore');

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

    const eventStore = process.env.EVENT_STORE_ROOT;

    const uri = `/streams/message-${message.sender}-${message.messageId}`;

    await publishEvent(`${eventStore}${uri}`, 'MessageReceived', message, metadata);
};
