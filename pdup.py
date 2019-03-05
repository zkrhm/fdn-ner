from glob import glob
import os
import pandas as pd
import spacy
from tinydb import TinyDB, Query
import numpy as np
from Levenshtein import jaro_winkler
from utils.similarity import JaroWinklerSim
from utils.store import StoreFactory, MemStore
from logging import Logger
import logging
import argparse
from datetime import datetime
# from numba import autojit, prange
# import numba
print("HELLO ANJENGG")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(name="FDN product dup")

CURDIR = os.path.dirname(os.path.abspath(__file__))
PRODUCT_DBF = os.path.join(CURDIR, 'db/products.json') #brand file path
# db = TinyDB(PRODUCT_DBF)

logger.debug("creating store and model")

HOST = '127.0.0.1'
STORETYPE = 'mem'

class ProductUnduplicate:

    def __init__(self):
        self.store = StoreFactory.create_store(host=HOST, port=3306, db='product_vec', storeType=STORETYPE)
        self.model = JaroWinklerSim(self.store)
        try:
            self.N = self.store.size()
            self.product_vec = None
        except Exception as e:
            print("oh fuck. {}".format(e))
        

    def read_dbf(self):
        # rdata = db.all()
        return self.store.N, [int(x) for x in self.store.all()]

    # @numba.jit(nopython=True, parallel=True)
    def load(self,sample=1.0):
        products = self.product_df(sample=sample)
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
                name = row['product_name']
                if pd.isna(name):
                    continue

                product = {
                    'id': row['product_id'], 
                    'brand_id': row['brand_id'],
                    'name': name,
                    'name_lower': str(name).lower()
                }
                
                self.store.store(product)
                i += 1
                logger.info("progress : {}%".format( round((i/n) * 100, 2)  ))
            # self.store.persist()

    def product_df(self,sample=1.0):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-products.csv'),header=0).sample(frac=sample)

    def brand_df(self):
        return pd.read_csv(os.path.join(CURDIR, 'data/ner-brands.csv'),header=0)

    def correlate(self):

        def pair(a, b):
            len_a = len(a)
            len_b = len(b)

            if len_a != len_b:
                raise Exception("len a shoule be equals to len b")
            i = 0
            for i in range(len_a):
                yield (a[i], b[i])

            
        logger.debug("product vector \n : {} \ngetting tril index".format(self.product_vec))
        iprod1, iprod2 = np.tril_indices(self.N)

        print("iprod1:{} , iprod2:{} ".format(iprod1, iprod2) )
        # print("pairs : ({})".format([x for x in zip(iprod1, iprod2)]))
        mprod = np.array((iprod1.astype(np.float16),iprod2.astype(np.float16)),dtype=np.float16)
        #transpose
        tprod = mprod.T
        t1 = datetime.now()
        for i,j in tprod:
            logger.debug("index : ({},{})".format(i, j))
            # self.model.update(i, j,lambda w: w.name_lower)
        t2 = datetime.now()
        print("time delta : {}".format((t2-t1).strftime('%H:%M:%S')))
            
        # raise Exception("WALLA")
        # for i in iprod1:
        #     for j in iprod2:
        #         print("(i:{}, j:{})".format(i,j))
        #         self.model.update(i, j,lambda w: w.name_lower)
        
        self.model.persist()

    def print_duplicates(self, score=0.5):
        res = self.store.keys(score)
        for x, y in res:
            prod1 = self.store.get(x)
            prod2 = self.store.get(y)
            print(
                "<PossibleDuplicates [PROD 1 : prod_name='{}' (ID={} BRAND={})] vs [PROD 2 : prod_name='{}' (ID={} BRAND={} )] >"\
                    .format(\
                        prod1.name, prod1.product_id, prod1.brand_id, \
                        prod2.name, prod2.product_id, prod2.brand_id) \
                )
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

class TableGen:

    @classmethod
    def generate(cls):
        store = StoreFactory.create_store(host=HOST, port=3306, db='product_vec', storeType=STORETYPE)
        store.generate()


if __name__ == "__main__":

    
    def get_engine():
        return ProductUnduplicate()
    print("HAI GOBLOOG")

    def initdb_action(args):
        print("generating..")
        TableGen.generate()
    
    def load_action(args):
        engine = get_engine()
        print('loading...')
        engine.load(sample=args.sample)

    def train_action(args):
        print('training..')
        engine = get_engine()
        engine.correlate()
    
    def printresult(args):
        print("results:")
        engine = get_engine()
        engine.print_duplicates(score=args.min_score)

    def default_action(args):
        print("masuk sini bangsaat..")

    parser = argparse.ArgumentParser(prog='cateye')
    parser.set_defaults(func=default_action)
    sparser = parser.add_subparsers(help="hello fuckers")
    
    gen_cmd = sparser.add_parser('initdb')
    gen_cmd.set_defaults(func=initdb_action)


    load_cmd = sparser.add_parser('load')
    load_cmd.add_argument('sample',type=float)
    load_cmd.set_defaults(func=load_action)

    train_cmd = sparser.add_parser('train')
    train_cmd.set_defaults(func=train_action)

    show_cmd = sparser.add_parser('show')
    show_cmd.add_argument('min-score',type=float)
    show_cmd.set_defaults(func=printresult)
    
    # engine.load(sample=0.5)
    # engine.correlate()
    # engine.print_duplicates(score=.75)
    # engine.main()

    args = parser.parse_args()
    args.func(args)