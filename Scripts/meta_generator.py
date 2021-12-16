#!/usr/bin/env python
# coding: utf-8

# In[1]:


from transformers import pipeline

"""
Helper class for generating summaries of the inputted corpus - Intended for meta description generation. 

Can be used for any meta generation based on the websites content
"""
class MetaGenerator:
    #Initiate with default model as t5-large
    def __init__(self,model = None):
        if not model:
            model = "t5-large"
        self.summarizer = pipeline("summarization",model = "t5-large",tokenizer = "t5-large")


    #Transforms single text
    def transform_single(self,text,min_length = 10,max_length = 40):
        return self.summarizer(text,min_length = min_length,max_length = max_length)[0]["summary_text"]

    #Transforms all lines in a corpus
    def transform(self,corpus,min_length = 10,max_length = 40,trim = True):
        return [self.transform_single(text, min_length,max_length) for text in corpus]



# In[ ]:




