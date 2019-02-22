from redis import Redis

class Store:
    def store(self, data:dict):
        pass
    def get(self, id):
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
        return self.client.get(id)

class TinyDbStore(Store):

    def __init__(self, kwargs):
        pass
    def store(self, data:dict):
        pass
    def get(self, id):
        pass