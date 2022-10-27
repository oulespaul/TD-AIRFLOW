import pyodbc
import pandas as pd
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('CONSISTENCY_DQ_CHECK',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def mssql_create_connection():
    connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=192.168.45.83,4070;UID=udlake;PWD=ekA@latad;TrustServerCertificate=yes;"
    cnxn = pyodbc.connect(connection_string)
    return cnxn

def insert_dq(cnxn,data):
    try:
        sql = """insert into DataQuality.dbo.consistency_dq_transaction(
            rule_id,database_name, schema_name,table_name, column_name ,key_column ,
            ref_table_name ,ref_column_name ,ref_key_column,
            cnt_unique,cnt_record,cnt_unique_join,cnt_record_join)
        values ({})""".format(','.join(data)).replace("'None'",'null').replace('None','null').replace('nan','null')
        cursor = cnxn.cursor()
        cursor.execute(sql)
        cnxn.commit()
        cursor.close()
    except:
        print('- !! Insert Failed')
        sys.exit(99)

def run_database_dq():

    cnxn = mssql_create_connection()

    sql = """select id as rule_id,database_name, schema_name ,table_name ,column_name ,key_column ,ref_table_name ,ref_column_name ,ref_key_column
    from DataQuality.dbo.dq_rules
    where rule_group = 'Consistency'"""

    df = pd.read_sql_query(sql, cnxn)
    cnt_rule = len(df)

    print("== Run Total Rule : {}".format(cnt_rule))
    print("==================================")

    for index, row in df.iterrows():

        print("{}) RuleID[{}] Table: {}.{} ----".format(index+1,row['rule_id'],row['database_name'],row['table_name']),end="")

        try:

            rule_id = row['rule_id']
            database_name = row['database_name']
            schema_name = row['schema_name']
            table_name = row['table_name']
            column_name = row['column_name']
            column_list = column_name.split(',')
            key_column = row['key_column']
            ref_table_name = row['ref_table_name']
            ref_column_name = row['ref_column_name']
            ref_column_list = ref_column_name.split(',')
            ref_key_column = row['ref_key_column']

            data = []
            data.append(str(rule_id))
            data.append("'"+str(database_name)+"'")
            data.append("'"+str(schema_name)+"'")
            data.append("'"+str(table_name)+"'")
            data.append("'"+str(column_name)+"'")
            data.append("'"+str(key_column)+"'")
            data.append("'"+str(ref_table_name)+"'")
            data.append("'"+str(ref_column_name)+"'")
            data.append("'"+str(ref_key_column)+"'")

            on_clause = ""
            for i,r in enumerate(column_list):
                on_clause = on_clause + " AND " if i > 0 else on_clause
                on_clause = on_clause + 'A.'+column_list[i]+' = B.'+ref_column_list[i]

            sql = """SELECT count(distinct A.{}) as CNT_UNIQUE,COUNT(A.{}) as CNT_RECORD,count(distinct B.{}) as CNT_UNIQUE_JOIN,count(B.{}) as CNT_RECORD_JOIN
                    FROM {}.dbo.{} A
                    LEFT JOIN {}.dbo.{} B
                    ON {}""".format(key_column,key_column,ref_key_column,ref_key_column,database_name,table_name,database_name,ref_table_name,on_clause)

            df_result = pd.read_sql_query(sql, cnxn)

            cnt_unique = df_result['CNT_UNIQUE'][0]
            cnt_record = df_result['CNT_RECORD'][0]
            cnt_unique_join = df_result['CNT_UNIQUE_JOIN'][0]
            cnt_record_join = df_result['CNT_RECORD_JOIN'][0]

            data.append(str(cnt_unique))
            data.append(str(cnt_record))
            data.append(str(cnt_unique_join))
            data.append(str(cnt_record_join))

            insert_dq(cnxn,data)

            print("> Successful")

        except Exception as e:
            print("> Failed")
            print("-------------------------------------")
            print(e)
            print("-------------------------------------")
            continue

with dag:
    run_consistency_dq_check = PythonOperator(
        task_id='run_consistency_dq_check',
        python_callable=run_database_dq,
    )

run_consistency_dq_check