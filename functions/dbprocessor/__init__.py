from google.cloud import datastore
import config


class DBProcessor(object):
    def __init__(self):
        self.client = datastore.Client()
        pass

    def process(self, payload):
        if 'gobits' in payload and 'when' in payload['gobits'] and 'data' in payload and \
                'vendor_number' in payload['data']:
            kind = config.DATASTORE_KIND

            entity_key = self.client.key(kind, payload['data']['vendor_number'])
            entity = self.client.get(entity_key)

            if entity is None:
                entity = datastore.Entity(key=entity_key)

            gobits = payload['gobits'][0] if isinstance(payload['gobits'], list) else payload['gobits']

            payload['data']['when'] = gobits['when']

            entity.update(payload['data'])
            self.client.put(entity)
