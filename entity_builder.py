import spacy
from glob import glob
import pandas as pd
import os

# print("hello")n
# nlp = spacy.load('en')

CURDIR = os.path.dirname(os.path.abspath(__file__))

def main():
    df = read_files(root='data/reviews*.csv')
    colnames = {
            0:'review_id',
            1:'brand_id',
            2:'product_id',
            3:'user_id',
            4:'posting_date',
            5:'username',
            6:'review_number',
            7:'review_content',
            8:'is_deleted'
    }

    df = df.rename(colnames, axis='columns') 
    df1 = df['review_content']
#     print(df1.head())

    '''
        TODO: loop to rows inside the df
        TODO: break each content into it's tokens
        TODO: find positions of each token (start and ends) and store somewhere.

        see you tomorrow.
    '''
    i = 0
    MAX_ROWS = 10
    for i, row in df.iterrows():
        print(row['review_content'])
        if i > MAX_ROWS:
            break

def read_files(root='*'):
    files = glob(root)
    df = None
    dfs = []
    for file in files:
        fpath = os.path.join(CURDIR, file)
        df = pd.read_csv(fpath,header=0)
        dfs.append(df)

    return pd.concat(dfs)
    

def to_token(doc):
    for token in doc:
        yield token

if __name__ == "__main__":
    main()