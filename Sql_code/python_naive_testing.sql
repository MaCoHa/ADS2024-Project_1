show DATABASEs;


use database CAT_DB;


create or replace function testdata(val variant)
returns table (text STRING, label INTEGER, predicted_label INTEGER)
language python
runtime_version=3.8
imports=('@stage_coha_csv/model.csv')
handler='testdata'
as $$
from collections import defaultdict
import csv
import os
import re
import sys


def remove_special_characters_and_numbers(text):
    # This will keep only alphabetic characters and spaces
        return re.sub(r'[^A-Za-z\s]', '', text)


class testdata:
    
    
        
    def read_model_data(self, path):
        with open(path, "r") as f:
            csvreader = csv.reader(f, delimiter=';')
            
            for (word, negative_probability, positive_probability, negative_count, positive_count, total_count) in csvreader:
                yield word, float(negative_probability), float(positive_probability), int(negative_count), int(positive_count), int(total_count)
        
    
    def __init__(self):
        self.processedlist = list()
        
        
        
        
    def process(self, val):
        if val["label"] == 0 or val["label"] == 4: 
            self.processedlist.append((val["label"], val["text"].lower()))
      
  


    def end_partition(self):
           
        
        model_file_path = os.path.join(sys._xoptions["snowflake_import_directory"], 'model.csv')
        words_probabilities = defaultdict(lambda:(0,0))
        words = set()
        negative_count = 0
        positive_count = 0
        total_count = 0
        
        for word, negative_probability, positive_probability,n,p,t in self.read_model_data(model_file_path):
            if negative_count == 0:
                negative_count = n
                positive_count = p
                total_count = t
            words.add(word)
            words_probabilities[word] = (negative_probability, positive_probability)
        
        total_prob_negative = negative_count / total_count
        total_prob_positive = positive_count / total_count
        
        
        for label,text  in self.processedlist:
            clean_string = remove_special_characters_and_numbers(text)
            clean_array = clean_string.split(" ")
            sum_pos = 1
            sum_neg = 1
            
            for j in clean_array:
                    pos, neg = words_probabilities[j]
                    if (pos == 0 or neg == 0):
                        continue
                    
                    sum_pos *= pos
                    sum_neg *= neg
            
            if ((total_prob_negative * sum_neg) < (total_prob_positive * sum_pos)):
                yield(text,label,0)
            else:
                yield(text,label,4)  
$$;


CREATE OR REPLACE TABLE prediction_python_table (
    text STRING,
    label INTEGER,
    predicted_label INTEGER
) AS
SELECT text,label,predicted_label 
FROM yelp_testing, 
TABLE(testdata(val) over ());

SELECT * FROM prediction_python_table;


CREATE OR REPLACE TABLE accuracy_table AS
SELECT
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN predicted_label = label THEN 1 ELSE 0 END) AS correct_predictions,
    (SUM(CASE WHEN predicted_label = label THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS accuracy_percentage
FROM prediction_python_table;


SELECT * FROM ACCURACY_TABLE;










