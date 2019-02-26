
from utils.store import Store
import numpy as np

from Levenshtein import jaro_winkler
import pickle
import logging

from logging import Logger
logger = logging.getLogger(name=__file__)

class Model:
    def update(self, idx, idy):
        pass

class JaroModel(Model):
    def __init__(self, store:Store):
        self.store = store
        self.N = self.store.size()
        self.vec = np.ndarray((self.N,self.N))


    def update(self, idx, idy):
        # ox = self.store.get(idx)
        # oy = self.store.get(idy)

        #save distance:
        # self.vec[idx,idy] = 
        pass



class JaroWinklerModel(Model):
    def __init__(self, store:Store):
        self.store = store

        
        self.N = self.store.size()
        self.vec = np.ndarray((self.N,self.N))
        logger.debug("vector with size ({},{}) is built ".format(self.N,self.N))


    def update(self, idx, idy, get_word):
        '''
            idx, idy : index x and y from enumerated shit
            get_word : a function to fetch the saved structure
        '''
        # idx = int(idx.encode('utf-8'))
        # idy = int(idy.encode('utf-8'))
        logger.debug("index : ({} TYPE={}, {} TYPE={})".format(idx, type(idx), idy, type(idy)))
        try:
            idx = int(idx)
            idy = int(idy)

            ox = self.store.get(str(idx))
            oy = self.store.get(str(idy))

            # logger.debug("object ox, oy : ({},{})".format(ox,oy))
            # logger.debug("vec : {}".format(self.vec))
            # logger.debug("N : {} from store type ({})".format(self.store.size(), type(self.store)))

            #save distance:
            if len(self.vec) == 0:
                self.N = self.store.size()
                self.vec = np.zeros((self.N, self.N))

            self.vec[idx,idy] = jaro_winkler(get_word(ox),get_word(oy))
        except Exception as e:
            logger.debug("(idx : {} type : {})".format(idx, type(idx)))
            raise e

    # def to_mdl_index(self, x, y):
    #     return 'midx:{}:{}'.format(int(x),int(y)) #midx for matrix index

    def save_to(self, storage: Store):
        x_idx, y_idx = self.vec.nonzero()
        for x,y in zip(x_idx,y_idx):

            sim_fx_val = self.vec[x,y].astype(float)
            x = int(x)
            y = int(y)

            print("x : {} , y : {} , f(x,y) : {}".format(x,y,sim_fx_val))
            v = {
                'x':x,
                'y':y,
                'sim': sim_fx_val
            }
            storage.store(v, key='midx:{}:{}'.format(x, y))
            storage.store(v,key='{}:{}:{}'.format(sim_fx_val, x, y))

    def dump_to(self, path):
        fd = open(path,'w')
        pickle.dump(self.vec, fd)
