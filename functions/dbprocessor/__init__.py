from google.cloud import datastore
import config


class DBProcessor(object):
    def __init__(self):
        self.client = datastore.Client()
        pass

    def process(self, payload):
        if 'when' in payload and 'data' in payload and \
                'vendor_number' in payload['data']:
            kind = config.DATASTORE_KIND

            entity_key = self.client.key(kind, payload['data']['vendor_number'])
            entity = self.client.get(entity_key)

            if entity is None:
                entity = datastore.Entity(key=entity_key)

            payload['data']['when'] = payload['when']

            entity.update(payload['data'])
            self.client.put(entity)
