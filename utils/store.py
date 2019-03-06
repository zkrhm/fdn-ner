from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging
from utils.model import SimVec, ProductLookup

logger = logging.getLogger(name=__file__)
metadata = MetaData()
sim_vec = Table('sim_vec', metadata,
    Column('id', Integer, primary_key=True),
    Column('method', String(255)),
    Column('x', Integer),
    Column('y', Integer),
    Column('val', Float),
)

sim_vec = Table('product_lookup', metadata,
    Column('id', Integer, primary_key=True),
    Column('product_id', Integer),
    Column('brand_id', Integer),
    Column('name', String(255)),
    Column('name_lower', String(4048))
)

class Store:
    def store(self, data:dict):
        pass
    def get(self, id):
        pass
    def size(self):
        pass
    def set_entry(self, x, y, val, method='jaro_wenkel'):
        pass
    def persist(self):
        pass
    def generate(self):
        pass

class StoreFactory:
    @classmethod
    def create_store(self,storeType:str, **kwargs):
        logger.debug('param entry : {}'.format(kwargs))
        SelectedStore =  {
            'redis':RedisStore,
            'tinydb':TinyDbStore,
            'mem':MemStore
        }[storeType]
        return SelectedStore(kwargs)

class MemStore:
    def __init__(self, kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.db = kwargs['db']
        # charset=utf8', encoding='utf-8'
        self.engine = create_engine('mysql+mysqldb://root@{}:{}/{}?charset=utf8'.format(self.host, self.port, self.db),encoding='utf-8')
        # self.conn = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def store(self, data):
        logger.debug(data)
        
        if pd.isna(data['name']):
            return

        o = ProductLookup(product_id = data['id'],brand_id=data['brand_id'],name=data['name'],name_lower=data['name_lower'])
        self.session.add(o)
        self.session.commit()

    def set_entry(self, x, y, val):
        e = SimVec(x=x, y=y, val=val)
        self.session.add(e)
        self.session.commit()

    def persist(self):
        self.session.commit()

    def get(self, id):
        return self.session.query(ProductLookup).filter(ProductLookup.id==id).first()

    def get_value(self, x, y):
        '''get value where index is (x,y)'''
        return self.session.query(SimVec).filter(SimVec.x==x).filter(SimVec.y==y).first().val 

    def keys(self, val):
        '''find key where value more than equal to val'''
        for e in self.session.query(SimVec).filter(SimVec.val >= val):
            yield (e.x, e.y)
        

    def all(self):
        for r in self.session.query(ProductLookup).all():
            yield r

    def size(self):
        return self.session.query(ProductLookup).count()

    def generate(self):
        metadata.create_all(self.engine)

class RedisStore:
    def __init__(self, kwargs):
        pass
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.db = kwargs['db'] or 0
        self.client = Redis(host=self.host, port=int(self.port), db=int(self.db))
        self.id = None
        self.N = int(self.stored_keys()['db0']['keys'])

    def store(self, data:dict, key=None):
        if key == None:
            self.client.hmset(self.N, data)
            self.N += 1
        else:
            self.client.hmset(key,data)

    def get(self, id):
        return self.client.hgetall(id)

    def all(self):
        resp = self.client.scan_iter('*')

        for r in resp:
            yield (r)

    def size(self):
        return self.N

    def stored_keys(self):
        return self.client.info()

class TinyDbStore(Store):

    def __init__(self, kwargs):
        pass
    def store(self, data:dict):
        pass
    def get(self, id):
        pass