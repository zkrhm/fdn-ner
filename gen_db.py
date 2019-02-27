from utils.store import StoreFactory, MemStore

def main():
    store = StoreFactory.create_store(host='naga.fdn', port='3306', db='product_vec', storeType='mem')
    print("generating database")
    store.generate()
    print("generating database done")

if __name__ == '__main__':
    main()