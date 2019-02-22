
from utils.store import Store
import numpy as np

from Levenshtein import jaro_winkler
import pickle

class Model:
    def update(self, idx, idy):
        pass

class JaroModel(Model):
    def __init__(self, store:Store):
        self.store = store
        self.N = self.store.size()
        self.vec = np.ndarray((self.N,self.N))


    def update(self, idx, idy):
        ox = self.store.get(idx)
        oy = self.store.get(idy)

        #save distance:
        # self.vec[idx,idy] = 

class JaroWinklerModel(Model):
    def __init__(self, store:Store):
        self.store = store
        self.N = self.store.size()
        self.vec = np.ndarray((self.N,self.N))


    def update(self, idx, idy, get_word):
        '''
            idx, idy : index x and y from enumerated shit
            get_word : a function to fetch the saved structure
        '''
        ox = self.store.get(idx)
        oy = self.store.get(idy)

        #save distance:
        self.vec[idx,idy] = jaro_winkler(get_word(ox),get_word(oy))
    
    def dump_to(self, path):
        fd = open(path,'w')
        pickle.dump(self.vec, fd)
