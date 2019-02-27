from glob import glob
import os
import pandas as pd
import spacy
from tinydb import TinyDB, Query
import numpy as np
from Levenshtein import jaro_winkler
from utils.ds_model import JaroWinklerSim
from utils.store import StoreFactory, MemStore
from logging import Logger
import logging
# from numba import autojit, prange
# import numba
print("HELLO ANJENGG")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(name="FDN product dup")

CURDIR = os.path.dirname(os.path.abspath(__file__))
PRODUCT_DBF = os.path.join(CURDIR, 'db/products.json') #brand file path
# db = TinyDB(PRODUCT_DBF)

logger.debug("creating store and model")

class ProductUnduplicate:

    def __init__(self):
        self.store = StoreFactory.create_store(host='naga.fdn', port=3306, db='product_vec', storeType='mem')
        self.model = JaroWinklerSim(self.store)
        self.N = self.store.size()
        self.product_vec = None

    def read_dbf(self):
        # rdata = db.all()
        return self.store.N, [int(x) for x in self.store.all()]

    # @numba.jit(nopython=True, parallel=True)
    def load(self):
        products = self.product_df(sample=0.125)
        # products = products.dropna()
        n = products.shape[0]
        i = 0

        print("N products = {}".format(n))
        Exception("Load break!")
        
        if self.N > 0:
            self.store.N = self.N
            logger.info('data available Store (N): {}'.format(self.N))
        else:
            logger.info("load data faker!!!")
            for _, row in products.iterrows():
                # print(row)
                product = {
                    'id': row['product_id'], 
                    'brand_id': row['brand_id'],
                    'name': row['product_name'],
                    'name_lower': str(row['product_name']).lower()
                }
                
                self.store.store(product)
                i += 1
                logger.info("progress : {}%".format( round((i/n) * 100, 2)  ))

    def product_df(self,sample=1.0):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-products.csv'),header=0).sample(frac=sample)

    def brand_df(self):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-brands.csv'),header=0)

    def correlate(self):
        logger.debug("product vector \n : {} \ngetting tril index".format(self.product_vec))
        x_idx, y_idx = np.tril_indices(self.N)
        for i in range(self.N):
            self.model.update(x_idx[i], y_idx[i],lambda w: w.name_lower)
        
        self.model.persist()


    def main(self):
        n = 0
        rdata = None

        self.N = int(self.store.size())
        logger.debug("keys : {}".format(self.store.size()))
            
        if self.N == 0:
            pass
       
        self.product_vec = np.zeros((self.N,self.N)) #brand vec is matrix of size NxN , N is number of 
        product_map = {}

        def get_word(w:dict):
            return w[b'name_lower'].decode('utf-8')

        logger.debug("CREATE & FILL the vector")
        self.correlate()

        raise Exception("End of training")
        
        # sim_vec = model.vec
        sim_vec = np.tril(self.model.vec,k=-1)
        dupl_idx = np.argwhere(sim_vec >= 1.).tolist()
        logger.info("suspected duplicates:")

        for dup_pair in dupl_idx:
            dupid0 = dup_pair[0]
            dupid1 = dup_pair[1]

            logger.info("[{}  (ID {})] vs [{} at (ID {})]".format(self.store.get(dupid0),dupid0,self.store.get(dupid1),dupid1))
        CURDIR = os.path.dirname(os.path.abspath(__file__))

        

if __name__ == "__main__":
    engine = ProductUnduplicate()
    # engine.load()
    engine.correlate()
    # engine.main()