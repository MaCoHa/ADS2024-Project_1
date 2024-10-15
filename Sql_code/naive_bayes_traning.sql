use database CAT_DB;

CREATE OR REPLACE TABLE clean_train_table (
    id INTEGER,
    label INTEGER,
    text ARRAY
) AS
SELECT
    ROW_NUMBER() OVER (ORDER BY val:label DESC) AS id, 
    val:label AS label,
    FILTER(
        SPLIT(LOWER(REGEXP_REPLACE(val:text, '[^\\w]+', ' ')), ' '),
        word -> word != ''
    ) AS text  
FROM 
    yelp_training 
WHERE 
    val:label = 0 OR val:label = 4;
CREATE OR REPLACE TABLE bagWithWords (
    label INT,
    word STRING,
    count INT
) AS SELECT
    label,
    word,
    COUNT(*) AS count
FROM (
    SELECT
        id,
        label,
        TRIM(FLATTENED.value) AS word  -- Flatten the array into individual words
    FROM clean_train_table,
         LATERAL FLATTEN(input => text) AS FLATTENED  -- Unnest the array
)
GROUP BY id, label, word
ORDER BY id, label, word;

-- 
CREATE OR REPLACE TABLE frequency_table (
    word String,
    used_negative INT,
    used_positive INT
) AS SELECT 
    word,
    SUM(CASE WHEN label = 0 THEN count ELSE 0 END) AS used_negative,
    SUM(CASE WHEN label = 4 THEN count ELSE 0 END) AS used_positive
FROM bagWithWords
GROUP BY word
ORDER BY word;


-- create probabilities table

CREATE OR REPLACE TABLE probabilities_table (
    word STRING,
    prob_negative FLOAT,
    prob_positive FLOAT
) AS SELECT 
    word,
        ((used_negative + 1) / ((SELECT SUM(used_negative) + COUNT(*)  FROM frequency_table))) 
    AS prob_negative ,
    ((used_positive + 1) / ((SELECT SUM(used_positive) + COUNT(*)  FROM frequency_table))) 
     AS prob_positive,
FROM frequency_table;

SELECT * FROM PROBABILITIES_TABLE;



