
"""
*********************************************************************************************
*
*   Some of this code is created by Andreas Titgen from 
*   https://github.com/Tietgen-ITU/advanced-data-systems/blob/main/exercise-01/scripts/sentiment/sentiment-benchmark.py
*   But have been modifyed to work for my bechmarking
*
*********************************************************************************************
"""

import snowflake.connector
import os
import qurries.Tpc_h_qurries as tpc
import csv

def create_connection(database_name, schema_name):
    password = os.getenv('SNOWSQL_PWD')

    snowflake_config = {
        "user": "CAT",
        "password": password,
        "account": "sfedu02-gyb58550",
        "database": database_name,
        "schema": schema_name,
        "session_parameters": {
            "USE_CACHED_RESULT": False
        }
    }

    return snowflake_config

def setup_warehouse(cur, wh_size):
    cur.execute("create or replace warehouse CAT_WH with WAREHOUSE_SIZE = " + wh_size + ";")
    cur.execute("use warehouse CAT_WH;")

def change_schema(cur, schema):
    cur.execute("use schema " + schema + ";")

def get_query_stats(cur, query_ids):
    query_ids_str = ', '.join([f"'{qid}'" for qid in query_ids])

    stats_query = f"""
    SELECT query_id, schema_name, warehouse_size, total_elapsed_time/1000 AS time_elapsed_in_seconds, total_elapsed_time AS total_elapsed_time_milli
    FROM
        table(information_schema.query_history())
    WHERE user_name = 'CAT' and execution_status = 'SUCCESS' and query_id IN ({query_ids_str})
    ORDER BY start_time desc;
    """
    
    cur.execute(stats_query)
    return cur.fetchall()

if __name__ == '__main__':
    warehouse_sizes = ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE']
    schemas = ['TPCH_SF1', 'TPCH_SF10', 'TPCH_SF100', 'TPCH_SF1000']
    queryid_to_query = {}
    stats = []
    repetitions = 3

    connection_config = create_connection("SNOWFLAKE_SAMPLE_DATA", "")
    conn = snowflake.connector.connect(**connection_config)
    cur = conn.cursor()
    try:
        for wh_size in warehouse_sizes:
            print("WAREHOUSE_SIZE: ", wh_size)
            setup_warehouse(cur, wh_size)
            query_ids = []

            for schema in schemas:
                print("SCHEMA: ", schema)
                change_schema(cur, schema)

                for idx, query in enumerate(tpc.queries):
                    print("Running query " + str(idx+1) + " out of 3")

                    # Run each query 6 times on each schema in each warehouse size
                    for i in range(repetitions):
                        status_print = "Executing query " + str(i+1) + " out of " + str(repetitions) + " times"
                        if i+1 == repetitions:
                            print(status_print)
                        else:
                            print(status_print, end='\r')
                        cur.execute(query)
                        qid = cur.sfqid
                        query_ids.append(qid) # Adds the query id to the list
                        queryid_to_query[qid] = idx

                    print()
            
            wh_query_stats = get_query_stats(cur, query_ids)
            for qid, schema, warehouse_size, elapsed_seconds, elapsed_milli in wh_query_stats:
                stats.append((qid, queryid_to_query[qid], schema, warehouse_size, float(elapsed_seconds), elapsed_milli))
        
        # Ouput the query stats in a CSV file
        with open('./benchmark_stats.csv', 'w') as file:
            writer = csv.writer(file, delimiter=';')

            for qid, queryid_type, schema, warehouse_size, elapsed_seconds, elapsed_milli in stats:
                writer.writerow((qid, queryid_type, schema, warehouse_size, elapsed_seconds, elapsed_milli))
        
    finally:
        conn.close()

    print("Benchmark results is written to 'benchmark_stats.csv'")