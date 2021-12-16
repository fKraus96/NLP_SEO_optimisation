#!/usr/bin/env python
# coding: utf-8

from nltk.corpus import stopwords
import pandas as pd
import spacy
import re


class Preprocessor:
    def __init__(self,data,pos_list,nlp, stop_lang = "english"):
        #Allow only dataframes
        if not isinstance(data,pd.DataFrame):
            raise Exception("Data needs to be of type dataframe")

        self.nlp = nlp
        #Set dataframe for preprocessing
        self.data = data

        #Set stopwordslist and parts of speech to filter for
        self.stopword_list = stopwords.words(stop_lang)
        self.pos_list = pos_list

    #Add to stopword list where needed
    def add_stopwords(self,l):
        self.stopword_list += l

    #Lemmatize and filter for POS and stopwords using df.apply
    def preprocess_text(self,col_name,out_col = "corpus_preprocessed"):
        self.data[out_col] = self.data[col_name].apply(self.__preprocess)
        self.data[out_col] = self.data[out_col].apply(self.__remove_unnecessary)

    #Helper function for spacy preprocessing
    def __preprocess(self,document):
        return " ".join([token.lemma_ for token in self.nlp(document) if token.pos_ in self.pos_list])

    #Helper function for removing redundant tokens - i.e. len <2 or bigger than 20
    def __remove_unnecessary(self,document):
        return " ".join([token.lower() for token in document.split() if len(token) >2 and len(token) <20 and token.lower() not in self.stopword_list])

    #Substitue pattern for dataframe
    def substitute(self,col_name, pattern, replacer = ""):
        #Compile to regex expression
        pattern = re.compile(pattern)
        #Apply re substitution to df
        self.data[col_name] = self.data[col_name].apply(lambda x: re.sub(pattern,replacer,x))

