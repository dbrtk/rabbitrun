
import json

import pika


class ConsumeMessage(object):
    """Consumes a JSON message. A subclass of this method has to be passed to
       AsyncConsumer. This subclass will have to implement a `process_msg`
       method.
    """
    def __init__(self,
                 properties: pika.spec.BasicProperties = None,
                 body: bytes = None):

        self.payload = json.loads(body)
        self.properties = properties

    def __call__(self):

        self.process_msg(payload=self.payload, properties=self.properties)

    def process_msg(self, payload: dict = None,
                    properties: pika.spec.BasicProperties = None):

        raise NotImplementedError()
 
