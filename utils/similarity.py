
from utils.store import Store
import numpy as np

from Levenshtein import jaro_winkler
from datetime import datetime
import pickle
import logging, time
from logging import Logger
logger = logging.getLogger(name=__file__)

class Similarity:
    def update(self, idx, idy):
        pass

class JaroSim(Similarity):
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

class JaroWinklerSim(Similarity):
    __method__ = 'jaro-winkler'
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
        # logger.debug("index : ({} TYPE={}, {} TYPE={})".format(idx, type(idx), idy, type(idy)))
        stime = datetime.now()
        try:
            idx = int(idx)
            idy = int(idy)

            if idx == idy :
                return

            ox = self.store.get(str(idx))
            oy = self.store.get(str(idy))

            if ox is None or oy is None:
                return
            w1 = get_word(ox)
            w2 = get_word(oy)
            simi = jaro_winkler(w1,w2)

            logger.debug("({},{}) vs ({},{}) : {}".format(idx, w1, idy, w2, simi))

            self.store.set_entry(idx,idy,simi)

        except Exception as e:
            logger.debug("(idx : {} type : {})".format(idx, type(idx)))
            raise e
        ntime = datetime.now()

        logger.debug("processing time : {}".format(ntime-stime))

    def persist(self):
        self.store.persist()

    def dump_to(self, path):
        fd = open(path,'w')
        pickle.dump(self.vec, fd)
