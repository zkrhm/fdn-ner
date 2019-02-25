
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
        print("vector with size ({},{}) is built ".format(self.N,self.N))


    def update(self, idx, idy, get_word):
        '''
            idx, idy : index x and y from enumerated shit
            get_word : a function to fetch the saved structure
        '''
        # idx = int(idx.encode('utf-8'))
        # idy = int(idy.encode('utf-8'))
        print("index : ({} TYPE={}, {} TYPE={})".format(idx, type(idx), idy, type(idy)))
        try:
            idx = int(idx)
            idy = int(idy)

            ox = self.store.get(str(idx))
            oy = self.store.get(str(idy))

            print("object ox, oy : ({},{})".format(ox,oy))
            print("vec : {}".format(self.vec))
            print("N : {} from store type ({})".format(self.store.size(), type(self.store)))
            #save distance:
            if len(self.vec) == 0:
                self.N = self.store.size()
                self.vec = np.ndarray((self.N, self.N))
            self.vec[idx,idy] = jaro_winkler(get_word(ox),get_word(oy))
        except Exception as e:
            print("(idx : {} type : {})".format(idx, type(idx)))
            raise e
        
    
    def dump_to(self, path):
        fd = open(path,'w')
        pickle.dump(self.vec, fd)
