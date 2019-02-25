from glob import glob
import os
import pandas as pd
import spacy
from tinydb import TinyDB, Query
import numpy as np
from Levenshtein import jaro_winkler
from utils.ds_model import JaroWinklerModel
from utils.store import StoreFactory
from numba import autojit, prange
import numba

CURDIR = os.path.dirname(os.path.abspath(__file__))
PRODUCT_DBF = os.path.join(CURDIR, 'db/products.json') #brand file path
# db = TinyDB(PRODUCT_DBF)

store = StoreFactory.create_store(host='localhost', port=6379, db=0, storeType='redis')
model = JaroWinklerModel(store)

def read_dbf():
    # rdata = db.all()
    return store.N, [int(x) for x in store.all()]

@numba.jit(nopython=True, parallel=True)
def load(products):
    n = products.shape[0]
    i = 0

    for idx, row in products.iterrows():
        # print(row)
        product = {
            'id': row['product_id'], 
            'brand_id': row['brand_id'],
            'name': row['product_name'],
            'name_lower': str(row['product_name']).lower()
        }
        store.store(product.values)
        i += 1
        print("progress : {}%".format( round((i/n) * 100, 2)  ))
        

def product_normalize():

    products = product_df()
    brands = brand_df()
    Product = Query
    # print(brands.head())

    load(products)
        # print("index : {} -> brand : {} , brand_lower : {}".format(idx, row['brands_item'], row['brands_item'].lower()))

    return read_dbf()

def product_df():
    return pd.read_csv(os.path.join(CURDIR, 'data/ner-products.csv'),header=0)

def brand_df():
    return pd.read_csv(os.path.join(CURDIR, 'data/ner-brands.csv'),header=0)

@autojit
def fill(store, model, n):

    for i in prange(store.size()):
        for j in prange(n):
            model.update(i,j,lambda w: w[b'name_lower'].decode('utf-8'))

    return store, model

if __name__ == "__main__":
    n = 0
    rdata = None
    if os.path.isfile(PRODUCT_DBF):
        resp = input('File Exists, Overwrite ? (Yes\\No): ')
        chosen = {
            'y':True,
            'yes': True,
            'n':False,
            'no': False
        }[resp.strip().lower()]

        if chosen :
            #remove file and reload
            os.remove(PRODUCT_DBF)
            n, rdata = product_normalize()
        else:
            n, rdata = read_dbf()
    else:
        n, rdata = product_normalize()
    
    print("rdata : \n {}".format(rdata))
    # #vectorize the brand.
    product_vec = np.ndarray((n,n)) #brand vec is matrix of size NxN , N is number of 
    product_map = {}
    # for i, obj in enumerate(rdata):

    
    #     product_map[i] = obj

    # print(product_map)

    def get_word(w:dict):
        return w[b'name_lower'].decode('utf-8')

    


    store, model = fill(store, model, n)
    

    # sim_vec = model.vec
    sim_vec = np.tril(model.vec,k=-1)
    dupl_idx = np.argwhere(sim_vec >= 1.).tolist()
    print("suspected duplicates:")

    for dup_pair in dupl_idx:
        dupid0 = dup_pair[0]
        dupid1 = dup_pair[1]

        print("[{}  (ID {})] vs [{} at (ID {})]".format(store.get(dupid0),dupid0,store.get(dupid1),dupid1))
    CURDIR = os.path.dirname(os.path.abspath(__file__))

    model.dump_to(os.path.join(CURDIR,'models/products-jero-wilkinks.bin'))
