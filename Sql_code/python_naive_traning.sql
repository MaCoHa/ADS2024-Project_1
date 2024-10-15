use database CAT_DB;


create or replace function traningdata(val variant)
returns table (word varchar, negative_prob float, positive_prob float, negative_count float, positive_count float, total_count float)
language python
runtime_version=3.8
handler='traningdata'
as $$
from collections import defaultdict
import re


def remove_special_characters_and_numbers(text):
    # This will keep only alphabetic characters and spaces
        return re.sub(r'[^A-Za-z\s]', '', text)

class traningdata:
    def __init__(self):
        self.word = ""
        self.PROB_NEGATIVE = float(0)
        self.PROB_POSITIVE = float(0)
        self.processedlist = list()
        
        
        
        
    def process(self, val):
        if val["label"] == 0 or val["label"] == 4:           
            self.processedlist.append((val["label"], val["text"].lower()))
      
  


    def end_partition(self):
      # sort words into bag
        dict = defaultdict(lambda: defaultdict(int))
        word_set = set()
        negative_count = 0
        positive_count = 0
        total_count = 0
        
        
        for label,text in self.processedlist:
           clean_string = remove_special_characters_and_numbers(text)
           clean_array = clean_string.split(" ")
           for j in clean_array:
                if j == "":
                  continue
              
                word_set.add(j)
                  
                total_count += 1
                
                if label == 0:
                    negative_count += 1
                elif (label == 4):
                    positive_count += 1
                    
                dict[j][label] = dict[j][label] + 1
                
       
        
        min_number = 1e-322
        
        for word in word_set:
            negative_prob = max(min_number,((dict[word][0] + 1) / (negative_count + len(word_set))))
            positive_prob = max(min_number,((dict[word][4] + 1) / (positive_count + len(word_set)))) 
            yield(word,negative_prob,positive_prob,negative_count,positive_count,total_count)  
        
        
               
$$;

DROP TABLE probabilities_python_table;
CREATE OR REPLACE TABLE probabilities_python_table (
    word STRING,
    prob_negative FLOAT,
    prob_positive FLOAT,
    negative_count FLOAT,
    positive_count FLOAT,
    total_count FLOAT
) AS
SELECT word,NEGATIVE_PROB,POSITIVE_PROB,negative_count,positive_count,total_count
FROM yelp_training,
TABLE(traningdata(val) over ());

create or replace file FORMAT coha_csv
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  COMPRESSION = NONE;


create or replace stage stage_coha_csv
    FILE_FORMAT = coha_csv;


COPY INTO @stage_coha_csv/model.csv
FROM probabilities_python_table
 single=true
 overwrite=true
 max_file_size=4900000000;

DROP TABLE IF EXISTS loaded_data;
 CREATE OR REPLACE TABLE loaded_data (
    word STRING,
    prob_negative FLOAT,
    prob_positive FLOAT,
    negative_count FLOAT,
    positive_count FLOAT,
    total_count FLOAT
);


COPY INTO loaded_data
FROM @stage_coha_csv/model.csv
FILE_FORMAT = (FORMAT_NAME = 'coha_csv');

SELECT * FROM loaded_data;






















  