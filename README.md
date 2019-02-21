# ner trainings

Author : zaky.rahim@gmail.com

## ENV setting

```python3 -m virtualenv env```
```source env/bin/activate```

## installation:

```pip install -r requirements.txt```

## run:

```python ner-training.py```


what you'll see ? it's **brand look alike (duplications because of typo)**

I purge the file. just use anggas's or mita's file . drop to data directory.. rename the file as:

- ner-brands.csv
- ner-products.csv

##to solve ambiguity I used __Jaro & Winkler Distance__

there's other method though... Jaro or Lehvenstein or other distance method. you just pick what you wanted.
code hasn't well structured it's still PoC.

Read the code, if you have question: I believe it will be a lot especially on matrix stuff (especially Ferdi Hasan & Bruno Mars)
you'll be using such matrix techniques for Product recommendation. so I think you should ask, I'll answer.

one question: 
```where you can use such distance?```
