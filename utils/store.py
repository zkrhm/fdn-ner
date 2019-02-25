from redis import Redis

class Store:
    def store(self, data:dict):
        pass
    def get(self, id):
        pass
    def size(self):
        pass

class StoreFactory:
    @classmethod
    def create_store(self,storeType:str, **kwargs):
        return {'redis':RedisStore(kwargs),'tinydb':TinyDbStore(kwargs)}[storeType]

class RedisStore:
    def __init__(self, kwargs):
        pass
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.db = kwargs['db'] or 0
        self.client = Redis(host=self.host, port=int(self.port), db=int(self.db))
        self.id = None
        self.N = 0

    def store(self, data:dict):
        self.client.hmset(self.N, data)
        self.N += 1

    def get(self, id):
        return self.client.hgetall(id)

    def all(self):
        resp = self.client.scan_iter('*')

        for r in resp:
            yield (r)

    def size(self):
        return self.N

class TinyDbStore(Store):

    def __init__(self, kwargs):
        pass
    def store(self, data:dict):
        pass
    def get(self, id):
        pass