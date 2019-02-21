import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span
import os

CURDIR = os.path.dirname(os.path.abspath(__file__))

class BrandMatcher(object):
    name = 'brand_matcher'

    def __init__(self, nlp, terms, label):
        patterns = [nlp.make_doc(text) for text in terms]
        self.matcher = PhraseMatcher(nlp.vocab)
        self.matcher.add(label, None, *patterns)

    def __call__(self, doc):
        matches = self.matcher(doc)
        for match_id, start, end in matches:
            span = Span(doc, start, end, label=match_id)
            doc.ents = list(doc.ents) + [span]
        return doc

class ProductMatcher(object):
    name = 'product_matcher'

    def __init__(self, nlp, terms, label):
        patterns = [nlp.make_doc(text) for text in terms]
        self.matcher = PhraseMatcher(nlp.vocab)
        self.matcher.add(label, None, *patterns)

    def __call__(self, doc):
        matches = self.matcher(doc)
        for match_id, start, end in matches:
            span = Span(doc, start, end, label=match_id)
            doc.ents = list(doc.ents) + [span]
        return doc

def load_brands():
    entities = ()
    with open(os.path.join(CURDIR, 'brands.list')) as fd:
        content:str = fd.read()
        entities = tuple(content.split("\t"))

    return entities
    

def load_products():
    entities = ()
    with open(os.path.join(CURDIR, 'products.list')) as fd:
        content:str = fd.read()
        entities = tuple(content.split("\t"))
    return entities

def build_brand_ner(terms):
    nlp = spacy.load('en')
    # terms = (u'cat', u'dog', u'tree kangaroo', u'giant sea spider')
    brand_matcher = BrandMatcher(nlp, terms, 'BRAND')

    nlp.add_pipe(brand_matcher, after='ner')
    # nlp.add_pipe(entity_matcher)
    return nlp
    # print(nlp.pipe_names)  # the components in the pipeline

    # doc = nlp(u"This is a text about Barack Obama and a tree kangaroo")
    # print([(ent.text, ent.label_) for ent in doc.ents])

def main():
    nlp = build_brand_ner(load_brands())
    # print(nlp.pipe_names)
    doc = nlp(u"Gue kemaren coba pake NUDE yang ini kerasanya lebih enak aja on my face, gak masalah meski jauh lebih murah daripada Estee")
    print("testing comment : {}".format(doc))
    print([(ent.text, ent.label_) for ent in doc.ents])


if __name__ == "__main__":
    main()