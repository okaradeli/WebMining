from TurkishStemmer import TurkishStemmer


def unit_test():
    stemmer = TurkishStemmer()
    x = stemmer.stem("fenerbahçe")
    print(x)

unit_test()
