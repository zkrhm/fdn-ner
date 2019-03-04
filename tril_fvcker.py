import numpy as np
import asyncio
import json

def async_test():
    sample = 0.5
    arr = np.arange(40000*sample)
    vecs = np.array((arr[::20][:10],arr[::50][:10]))
    tvecs = vecs.T

    async def the_func(params):
        oparams = json.loads(params)
        for p in oparams:
            print("processing : {}".format(p))
            await asyncio.sleep(1.)

    coros = []
    oarr = np.array_split(tvecs,4)
    for o in oarr:
        o_arr = np.array(o)
        o_json = json.dumps(o_arr.tolist())
        print("o[arr] : {} , type: {}".format(o_arr, type(o_arr)))
        print("o[json] : {}, type : {}".format(o_json, type(o_json)))
        coros.append(the_func(o_json))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(
        coros
    ))
    loop.close()

def split_test():
    sample = 0.5
    arr = np.arange(40000*sample)
    vecs = np.array((arr[::20][:10],arr[::50][:10]))
    tvecs = vecs.T
    print(vecs)
    print("transposed:\n\n{}".format(tvecs))
    def fn(p,q):
        return p+q
    vfn = np.vectorize(fn)

    print("sianjing:\n\n\n{}".format(np.array_split(tvecs,4)))

def tril_test():
    sample = 1.0
    tril = np.tril_indices(int(40000*sample),k=-1)
    print("tril :\n{}\n{} ".format(tril[0],tril[1]))
    pair = np.array((tril[0],tril[1]))
    # print("dstack :\n{}".format(np.dstack(tril)))

def tolist_test():
    # x = np.ndarray([
    #     np.array([
    #     0,1,2,3
    # ]),np.array([
    #     0,1,2,3
    # ])])

    x , y= np.arange(9.0), np.arange(9.0)
    z = np.array((x,y))
    a = np.arange(100.)
    b = np.array(np.split(a,5))
    print("b : {} , type: {}".format(b, type(b)))
    print(json.dumps(b.tolist()))
    # print()

if __name__ == "__main__":
    async_test()

    # print("first pair :\n{}".format(pair.T))
    # for i in tril: 
    #     print(i)

    
    # print('vectorize :\n\n{}'.format(vfn(tvecs)))

    