'use strict';

const memoize = require('fast-memoize');

const {Base64} = require('js-base64');

const AWS = require('aws-sdk');

const fetch = require('node-fetch');

const uuid = require('uuid/v4');

module.exports.populateEntity = async (target) => {
    const headers = {
        accept: 'application/vnd.eventstore.atom+json',
        authorization: (await authentication())
    };

    const headOfStreamResponse = await fetch(target.streamUri, {headers});

    if (headOfStreamResponse.ok) {

        let eventsPageUri = target.streamUri;

        const headOfStream = await headOfStreamResponse.json();

        const startOfStream = headOfStream.links.find(l => l.relation === 'last');

        if (startOfStream) {
            eventsPageUri = startOfStream.uri;
        }

        let eventsPage;

        do {
            const eventsResponse = await fetch(`${eventsPageUri}?embed=body`, {headers});

            eventsPage = await eventsResponse.json();

            eventsPage.entries.reverse().forEach(e => target[`apply${e.eventType}`](JSON.parse(e.data)));

            eventsPageUri = eventsPage.links.find(l => l.relation === 'previous').uri;
        } while (!eventsPage.headOfStream)
    }
};

module.exports.publishEvent = async (streamUri, eventType, data, metadata) => {
    const headers = {
        'accept': 'application/json',
        'content-type': 'application/vnd.eventstore.events+json',
        'authorization': (await authentication()),
    };

    const eventsBody = [
        {
            eventId: uuid(),
            eventType,
            data,
            metadata
        }
    ];

    const eventResponse = await fetch(streamUri, {
        headers,
        method: 'POST',
        body: JSON.stringify(eventsBody)
    });

    console.log(`${eventResponse.ok} - ${eventResponse.status} ${eventResponse.statusText}`);
};

const fetchEntry = async (entryUri) => {
    const headers = {
        accept: 'application/vnd.eventstore.atom+json',
        authorization: (await authentication())
    };

    const entryResponse = await fetch(entryUri, {headers});

    return entryResponse.json();
};

const relationUri = (entry, relation) => {
    return entry.links.find((l) => l.relation === relation).uri
};

const resolveEvent = async (entry) => {
    if (entry.summary === '$>') {
        const alternateUri = relationUri(entry, 'alternate');
        const alternateEntry = await fetchEntry(alternateUri);
        return resolveEvent(alternateEntry);
    } else {
        return entry.content;
    }
};

module.exports.fetchSubscriptionEvent = async (subscriptionUri) => {
    const headers = {
        accept: 'application/vnd.eventstore.competingatom+json',
        authorization: (await authentication())
    };

    const subscriptionResponse = await fetch(subscriptionUri, {headers});

    const subscription = await subscriptionResponse.json();

    if (subscription.entries.length) {
        const eventEntry = subscription.entries[0];

        const ack = relationUri(eventEntry, 'ack');
        const nack = relationUri(eventEntry, 'nack');

        const event = await resolveEvent(eventEntry);

        return {event, ack, nack};
    }
    return undefined;
};

module.exports.ackSubscription = async (subscriptionEvent) => {
    const headers = {
        authorization: (await authentication())
    };

    await fetch(subscriptionEvent.ack, {
        headers,
        method: 'POST'
    })
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
