use database CAT_DB;


CREATE OR REPLACE TABLE clean_test_table (
    id INTEGER,
    label INTEGER,
    text ARRAY
) AS SELECT
 ROW_NUMBER() OVER (ORDER BY val:label DESC) AS id, 
    val:label AS label,
    FILTER(
        SPLIT(LOWER(REGEXP_REPLACE(val:text, '[^\\w]+', ' ')), ' '),
        word -> word != ''
    ) AS text  
FROM 
    yelp_testing 
WHERE 
    val:label = 0 OR val:label = 4;



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

set min_number = 1e-322;

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
    IFF(p.prob_negative >= $min_number, p.prob_negative , $min_number ),
    IFF(p.prob_positive >= $min_number, p.prob_positive , $min_number )
    
FROM BAG_WITH_TEST_WORD t
INNER JOIN probabilities_table p
    ON t.word = p.word;

SELECT * FROM TEST_WORD_PROB_TABLE;


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


CREATE OR REPLACE TABLE accuracy_table AS
SELECT
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN prediction = label THEN 1 ELSE 0 END) AS correct_predictions,
    (SUM(CASE WHEN prediction = label THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS accuracy_percentage
FROM predictions_table;




SELECT * FROM ACCURACY_TABLE;

/*
SELECT * FROM PREDICTIONS_TABLE order by id;
SELECT * FROM clean_test_table;
SELECT * FROM clean_train_table;
SELECT * FROM BAGWITHWORDS;
SELECT * FROM BAG_WITH_TEST_WORD;
SELECT * FROM FREQUENCY_TABLE;
SELECT * FROM PROBABILITIES_TABLE;
SELECT * FROM reviews_table;
SELECT * FROM test_table;
*/