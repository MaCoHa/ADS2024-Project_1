__train_table = 'yelp_training'






udtf_train ="""
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
"""


udtf_query = """
CREATE OR REPLACE TABLE prediction_python_table (
    text STRING,
    label INTEGER,
    predicted_label INTEGER
) AS
SELECT text,label,predicted_label 
FROM yelp_testing, 
TABLE(testdata(val) over ());
"""