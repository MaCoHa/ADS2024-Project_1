

sql_train_queries = [
"""
CREATE OR REPLACE TABLE clean_train_table (
    id INTEGER,
    label INTEGER,
    text ARRAY
) AS
SELECT
    ROW_NUMBER() OVER (ORDER BY val:label DESC) AS id, 
    val:label AS label,
    FILTER(
        SPLIT(clean_string(val:text), ' '),
        word -> word != ''
    ) AS text  
FROM yelp_training 
WHERE 
    val:label = 0 OR val:label = 4;
""",
"""
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
""",
"""
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
""",
"""
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
"""
]



sql_test_queries = [
"""
    CREATE OR REPLACE TABLE clean_test_table (
    id INTEGER,
    label INTEGER,
    text ARRAY
) AS SELECT
 ROW_NUMBER() OVER (ORDER BY val:label DESC) AS id, 
    val:label AS label,
    FILTER(
        SPLIT(clean_string(val:text), ' '),
        word -> word != ''
    ) AS text 
FROM 
    yelp_testing 
WHERE 
    val:label = 0 OR val:label = 4;              
""",
"""
CREATE OR REPLACE TABLE bag_with_test_word (
    id INT,
    label INT,
    word STRING
)
AS SELECT
    id,
    label,
    word,
FROM (
    SELECT
        id,
        label,
        TRIM(FLATTENED.value) AS word  
    FROM clean_test_table,
         LATERAL FLATTEN(input => text) AS FLATTENED 
);
""",
"""
Set min_number = 1e-322;

""",
"""
CREATE OR REPLACE TABLE TEST_WORD_PROB_TABLE(
    id,
    label,
    word,
    prob_negative FLOAT,
    prob_positive FLOAT
) AS SELECT
    id,
    label,
    t.word,
    IFF(p.prob_negative >= 1e-322, p.prob_negative , 1e-322 ),
    IFF(p.prob_positive >= 1e-322, p.prob_positive , 1e-322 )
    
FROM BAG_WITH_TEST_WORD t
INNER JOIN probabilities_table p
    ON t.word = p.word;
""",
"""
CREATE OR REPLACE TABLE adjusted_probabilities
(
    id INT,
    label INT,
    negative_prob FLOAT,
    positive_prob FLOAT

) AS
    SELECT 
        t.id,
        t.label,
        EXP(SUM(LN(prob_negative))),
        EXP(SUM(LN(prob_positive))) 
    FROM TEST_WORD_PROB_TABLE t
    GROUP BY t.id, t.label;
""",
"""
CREATE OR REPLACE TABLE predictions_table (
    id INTEGER,
    label INTEGER,
    prediction INTEGER
) AS
WITH proportions AS (
    SELECT 
        SUM(used_negative) / (SUM(used_negative) + SUM(used_positive)) AS negative_proportion,
        SUM(used_positive) / (SUM(used_negative) + SUM(used_positive)) AS positive_proportion
    FROM frequency_table
) SELECT
    t.id,
    t.label,
    CASE 
        WHEN (p_props.negative_proportion * t.negative_prob) 
             > (p_props.positive_proportion * t.positive_prob)
        THEN 0
        ELSE 4
    END AS prediction
FROM adjusted_probabilities as t
CROSS JOIN proportions p_props;
"""]
    
    
    

