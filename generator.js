'use strict';

const AWS = require('aws-sdk');

const randomNumber = (size) => () => Math.floor(Math.random() * size);

const senderId = randomNumber(100);

const assetCount = randomNumber(5);

const generate = () => {
    const sender = senderId();

    const created = new Date();

    const messageId = created.getTime();

    const assets = [...Array(assetCount() + 1).keys()]
        .map((i) => i + 1)
        .map((i) => ({
            index: i,
            location: `resources/${i}.bin`
        }));

    const message = {
        messageId,
        sender,
        created: created.toISOString(),
        assets
    };

    const s3 = new AWS.S3();

    const bucket = process.env.BUCKET_NAME;

    const deliveryLocation = `deliveries/${sender}/${messageId}`;

    const messageKey = `${deliveryLocation}/message.json`;

    return Promise.all(assets.map((a) => s3.putObject({
        Bucket: bucket,
        Key: `${deliveryLocation}/${a.location}`,
        Body: `Asset ${a.index}`
    }).promise()))
        .then(() => s3.putObject({
                Bucket: bucket,
                Key: messageKey,
                Body: JSON.stringify(message),
                ContentType: 'application/json'
            }).promise()
        );
};

module.exports.deliveries = async (event, context) => {
    const interval = 10000;

    await generate();

    const scheduledTask = new Promise((resolve) => {
        const deliverSchedule = setInterval(async () => {
            await generate();
            if (context.getRemainingTimeInMillis() < interval) {
                clearInterval(deliverSchedule);
                resolve();
            }
        }, interval);
    });

    await scheduledTask;

    console.log('Done');
};