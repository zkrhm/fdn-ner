from glob import glob
import os
import pandas as pd
import spacy
from tinydb import TinyDB, Query
import numpy as np
from Levenshtein import jaro_winkler

CURDIR = os.path.dirname(os.path.abspath(__file__))
BRAND_DBF = os.path.join(CURDIR, 'db/brands.json') #brand file path
db = TinyDB(BRAND_DBF)


def read_dbf():
    rdata = db.all()
    return len(rdata), rdata

def brand_normalize():

    products = product_df()
    brands = brand_df()
    Brand = Query
    # print(brands.head())
    for idx, row in brands.iterrows():

        brand = {
            'id': row['brands_id'], 
            'name': row['brands_item'], 
            'name_lower':row['brands_item'].lower()
        }

        db.insert(brand)
        print(brand)
        # print("index : {} -> brand : {} , brand_lower : {}".format(idx, row['brands_item'], row['brands_item'].lower()))

    return read_dbf()

def product_df():
    return pd.read_csv(os.path.join(CURDIR, 'data/ner-products.csv'),header=0)

def brand_df():
    return pd.read_csv(os.path.join(CURDIR, 'data/ner-brands.csv'),header=0)

if __name__ == "__main__":
    n = 0
    rdata = None
    if os.path.isfile(BRAND_DBF):
        resp = input('File Exists, Overwrite ? (Yes\\No): ')
        chosen = {
            'y':True,
            'yes': True,
            'n':False,
            'no': False
        }[resp.strip().lower()]

        if chosen :
            #remove file and reload
            os.remove(BRAND_DBF)
            n, rdata = brand_normalize()
        else:
            n, rdata = read_dbf()
    else:
        n, rdata = brand_normalize()
    
    #vectorize the brand.
    brand_vec = np.ndarray((n,n)) #brand vec is matrix of size NxN , N is number of 
    brand_map = {}
    for i, obj in enumerate(rdata):
        brand_map[i] = obj

    print(brand_map)

    for i in range(n):
        for j in range(n):
            #sim checking
            # print(brand)
            brand_vec[i][j] = jaro_winkler(brand_map[i]['name_lower'], brand_map[j]['name_lower']) #sim check 


    sim_vec = np.tril(brand_vec,k=-1)
    dupl_idx = np.argwhere(sim_vec >= 1.).tolist()
    print("suspected duplicates:")

    for dup_pair in dupl_idx:
        brand_0 = dup_pair[0]
        brand_1 = dup_pair[1]

        print("({} at {}) and ({} at {})".format(brand_map[brand_0],brand_0,brand_map[brand_1],brand_1))
