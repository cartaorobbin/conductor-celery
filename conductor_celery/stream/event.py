from pyramid.events import subscriber
import json


class KafkaEvent(object):
    def __init__(self, request, topic, **kwargs):
        self.request = request
        self.topic = topic
        self.kwargs = kwargs


@subscriber(KafkaEvent)
def send(event):
    # index the document using our application's index_doc function
    event.request.registry.kafka.producer.send(
        event.topic, json.dumps(event.kwargs).encode("utf-8")
    )