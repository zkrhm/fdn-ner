from glob import glob
import os
import pandas as pd
import spacy
from tinydb import TinyDB, Query
import numpy as np
from Levenshtein import jaro_winkler
from utils.ds_model import JaroWinklerModel
from utils.store import StoreFactory
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
        self.store = StoreFactory.create_store(host='localhost', port=6379, db=0, storeType='redis')
        self.model = JaroWinklerModel(self.store)
        self.N = 0
        self.product_vec = None

    def read_dbf(self):
        # rdata = db.all()
        return self.store.N, [int(x) for x in self.store.all()]

    # @numba.jit(nopython=True, parallel=True)
    def load(self):
        products = self.product_df()
        n = products.shape[0]
        i = 0
        
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
                self.store.store(product.values)
                i += 1
                logger.info("progress : {}%".format( round((i/n) * 100, 2)  ))

    def product_df(self):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-products.csv'),header=0)

    def brand_df(self):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-brands.csv'),header=0)

    # @autojit


    def fill(self):
        logger.debug("product vector \n : {} \ngetting tril index".format(self.product_vec))
        x_idx, y_idx = np.tril_indices(self.N)
        for i in range(self.N):
            self.model.update(x_idx[i], y_idx[i],lambda w: w[b'name_lower'].decode('utf-8'))

        #ieu kudu dibenerkeun kulantaran lamun henteu pasalingsingan jeung naon? teuing.
        #
        self.model.save_to(StoreFactory.create_store(host='localhost', port=6379, db=1, storeType='redis'))
        # self.model.dump_to(os.path.join(CURDIR,'models/products-jero-wilkinks.bin'))

    def main(self):
        n = 0
        rdata = None

        self.N = int(self.store.stored_keys()['db0']['keys'])
        logger.debug("keys : {}".format(self.N))
        
        if self.N == 0:
            logger.debug("LOADING DATA")
            self.load()

        # if os.path.isfile(PRODUCT_DBF):
        #     resp = input('File Exists, Overwrite ? (Yes\\No): ')
        #     chosen = {
        #         'y':True,
        #         'yes': True,
        #         'n':False,
        #         'no': False
        #     }[resp.strip().lower()]

        #     if chosen :
        #         #remove file and reload
        #         os.remove(PRODUCT_DBF)
        #         n, rdata = self.product_normalize()
        #     else:
        #         n, rdata = self.read_dbf()
        # else:
        #     n, rdata = self.product_normalize()
        
        # logger.debug("rdata : \n {}".format(rdata))
        # #vectorize the brand.
        logger.debug("creating product vector")
        self.product_vec = np.zeros((self.N,self.N)) #brand vec is matrix of size NxN , N is number of 
        product_map = {}
        # for i, obj in enumerate(rdata):
        #     product_map[i] = obj

        # print(product_map)

        def get_word(w:dict):
            return w[b'name_lower'].decode('utf-8')
        logger.debug("FILL the vector")
        self.fill()
        

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
    engine.main()