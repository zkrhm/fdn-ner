from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class SimVec(Base):
    __tablename__ = 'sim_vec'
    id = Column(Integer, primary_key=True)
    method = Column(String(255), default='jaro-wenkel')
    x = Column(Integer)
    y = Column(Integer)
    val = Column(Float)

    def __repr__(self):
       return "<SimilarityVector(id='{}', x='{}', y='{}', val='{}')>".format (self.id, self.x, self.y, self.val)

class ProductLookup(Base):
    __tablename__ = 'product_lookup'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    brand_id = Column(Integer)
    name = Column(String(255))
    name_lower = Column(String(4048))

    def __repr__(self):
       return "<Product(id='{}', brand_id='{}', name='{}', name_lower='{}')>".format (self.product_id, self.brand_id, self.name, self.name_lower)