import numpy as np
if __name__ == "__main__":
    sample = 1.0
    tril = np.tril_indices(int(40000*0.125),k=-1)
    print("tril :\n{}\n{} ".format(tril[0],tril[1]))
    pair = np.array((tril[0],tril[1]))
    # print("dstack :\n{}".format(np.dstack(tril)))

    print("first pair :\n{}".format(pair.T))
    # for i in tril: 
    #     print(i)