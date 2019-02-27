from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(name=__file__)

class Store:
    def store(self, data:dict):
        pass
    def get(self, id):
        pass
    def size(self):
        pass
    def set_entry(self, x, y, val):
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

metadata = MetaData()
sim_vec = Table('sim_vec', metadata,
    Column('id', Integer, primary_key=True),
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

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
from sqlalchemy import Column, Integer, String
class SimVec(Base):
    __tablename__ = 'sim_vec'

    id = Column(Integer, primary_key=True)
    x = Column(Integer)
    y = Column(Integer)
    val = Column(Float)

    def __repr__(self):
       return "<SimilarityVector(id='{}', x='{}', y='{}', val='{}')>".format (self.id, self.x, self.y, self.val)

class ProductLookup(Base):
    __tablename__ = 'product_lookup'
    #'id': row['product_id'], 
    #'brand_id': row['brand_id'],
    #'name': row['product_name'],
    #'name_lower': str(row['product_name']).lower()
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    brand_id = Column(Integer)
    name = Column(String(255))
    name_lower = Column(String(4048))

    def __repr__(self):
       return "<Product(id='{}', brand_id='{}', name='{}', name_lower='{}')>".format (self.product_id, self.brand_id, self.name, self.name_lower)


class MemStore:
    def __init__(self, kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.db = kwargs['db']

        self.engine = create_engine('mysql+mysqldb://root@{}:{}/{}'.format(self.host, self.port, self.db))
        # self.conn = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def store(self, data):
        logger.debug(data)
        o = ProductLookup(product_id = data['id'],brand_id=data['brand_id'],name=data['name'],name_lower=data['name_lower'])
        self.session.add(o)
        self.session.commit()

    def set_entry(self, x, y, val):
        e = SimVec(x=x, y=y, val=val)
        self.session.add(e)

    def persist(self):
        self.session.commit()

    def get(self, id):

        return self.session.query(ProductLookup).filter(ProductLookup.id==id).first()

    def get_value(self, x, y):
        '''get value where index is (x,y)'''
        return self.session.query(SimVec).filter(SimVec.x==x).filter(SimVec.y==y).first().val 

    def keys(self, val):
        '''find key where value == val'''
        xs = []
        ys = []
        for e in self.session.query(SimVec).filter(SimVec.val==val):
            xs.append(e.x)
            ys.append(e.y)
        return xs,ys

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