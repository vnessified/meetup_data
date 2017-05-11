#!/usr/bin/env python3

import json
import requests
from requests.exceptions import ChunkedEncodingError
import boto3
import datetime


def initialize():
    kinesis = boto3.client('firehose', region_name='us-east-1')
    return kinesis


def stream_events(stream_request, client, stream_name):
    # get streaming event data from meetup.com
    print(stream_request.status_code)
    data = stream_request.iter_lines()
    # event = None
    try:
        for event in data:
            client.put_record(
                DeliveryStreamName=stream_name,
                Record={'Data': event.decode('utf-8') + '\n'})

    except Exception as e:
        if not event:
            event = 'Not defined'
        print('triggered {} exception (event, time): {}, {}'.format(type(e).__name__, event, datetime.datetime.now())


if __name__ == '__main__':
    kinesis = initialize()
    while True:
        print('reconnecting at {}'.format(datetime.datetime.now()))
        meetup_request = requests.get('http://stream.meetup.com/2/open_events',
                                      stream=True)
        stream_events(meetup_request, kinesis, 'meetupeventstream')
