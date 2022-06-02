import argparse
import csv
import time

from pyspark.sql import SparkSession
from PysparkBenchReport import PysparkBenchReport

from check import get_abs_path

INSERT_FUNCS = ['LF_CR',
            'LF_CS',
            'LF_I',
            'LF_SR',
            'LF_SS',
            'LF_WR',
            'LF_WS']
DELETE_FUNCS = ['DF_CS',
                'DF_SS',
                'DF_WS']
INVENTORY_DELETE_FUNC = ['DF_I']
DM_FUNCS = INSERT_FUNCS + DELETE_FUNCS + INVENTORY_DELETE_FUNC

def get_delete_date(spark_session):
    """get delete dates for Data Maintenance. Each delete functions requires 3 tuples: (date1, date2)

    Args:
        spark_session (SparkSession): Spark session
    Returns:
        delete_dates_dict ({str: list[(date1, date2)]}): a dict contains date tuples for each delete functions
    """
    delete_dates = spark_session.sql("select * from delete").collect()
    inventory_delete_dates = spark_session.sql("select * from inventory_delete").collect()
    date_dict = {}
    date_dict['delete'] = [(row['date1'], row['date2']) for row in delete_dates]
    date_dict['inventory_delete'] = [(row['date1'], row['date2']) for row in inventory_delete_dates]
    return date_dict

def replace_date(query_list, date_tuple_list):
    """Replace the date keywords in DELETE queries. 3 date tuples will be applied to the delete query.

    Args:
        query_list ([str]): delete query list
        date_tuple_list ([(str, str)]): actual delete date
    """
    q_updated = []
    for date_tuple in date_tuple_list:
        for c in query_list:
            c = c.replace("DATE1", date_tuple[0])
            c = c.replace("DATE2", date_tuple[1])
            q_updated.append(c)
    return q_updated

def get_maintenance_queries(folder, spec_queries):
    """get query content from DM query files

    Args:
        folder (str): folder to Data Maintenance query files
        spec_queries (list[str]): specific target Data Maintenance queries
    Returns:
        dict{str: list[str]}: a dict contains Data Maintenance query name and its content.
    """
    # need a spark session to get delete date
    spark = SparkSession.builder.appName("GET DELETE DATES").getOrCreate()
    delete_date_dict = get_delete_date(spark)
    # exclude this "get_delte_date" step from main DM process.
    spark.stop()
    global DM_FUNCS
    if spec_queries:
        for q in spec_queries:
            if q not in DM_FUNCS:
                raise Exception(f"invalid Data Maintenance query: {q}. Valid  are: {DM_FUNCS}")
        DM_FUNCS = [q for q in spec_queries if q in DM_FUNCS]
    folder_abs_path = get_abs_path(folder)
    q_dict = {}
    for q in DM_FUNCS:
        with open(folder_abs_path + '/' + q + '.sql', 'r') as f:
            # file content e.g.
            # " CREATE view ..... ; INSERT into .... ;"
            # " DELETE from ..... ; DELETE FROM .... ;"
            q_content = [ c + ';' for c in f.read().split(';')[:-1]]
            if q in DELETE_FUNCS:
                # There're 3 date tuples to be replace for one DELETE function
                # according to TPC-DS Spec 5.3.11
                q_content = replace_date(q_content, delete_date_dict['delete'])
            if q in INVENTORY_DELETE_FUNC:
                q_content = replace_date(q_content, delete_date_dict['inventory_delete'])
            q_dict[q] = q_content
    return q_dict

def run_insert_query(spark, query_list):
    """Run insert query. Insert query contains 3 subqueries.
    See data_maintenance/LF_*.sql for details.

    Args:
        spark (SparkSession):  SparkSession instance.
        query_list ([str]): INSERT query list.
    """
    for q in query_list:
        spark.sql(q)

def run_delete_query(spark, query_name, query_list):
    """Only process DELETE queries as Spark doesn't support it yet.
    See https://github.com/NVIDIA/spark-rapids-benchmarks/pull/9#issuecomment-1141956487.
    This function runs subqueries first to get subquery results. Then run the main delete query
    using the intermediate results.
    # See data_maintenance/DF_*.sql for query details.

    Args:
        spark (SparkSession): SparkSession instance.
        query_name (str): query name to identify DELETE or INVENTORY DELETE queries.
        query_list ([str]): DELETE query list. The first 3 queires are subquery results.
                            The last 2 queries are main DELETE queries.
    """
    if query_name in DELETE_FUNCS:
        # contains 3 subqueries
        # a list "[1,2,3,4,5]", drop box brackets for further SQL process.
        list_result = str([x[0] for x in spark.sql(query_list[0]).collect()])[1:-1]
        # date results are integer
        date1 = str(spark.sql(query_list[1]).collect()[0][0])
        date2 = str(spark.sql(query_list[2]).collect()[0][0])
        main_delete_1 = query_list[3].replace("SQL1", list_result)
        main_delete_2 = query_list[4].replace("SQL2", date1)
        main_delete_2 = main_delete_2.replace("SQL3", date2)
        spark.sql(main_delete_1)
        spark.sql(main_delete_2)
    if query_name in INVENTORY_DELETE_FUNC:
        # contains 2 subqueries
        date1 = str(spark.sql(query_list[0]).collect()[0][0])
        date2 = str(spark.sql(query_list[1]).collect()[0][0])
        main_delete = query_list[4].replace("SQL1", date1)
        main_delete = main_delete.replace("SQL2", date2)
        spark.sql(main_delete)

def run_query(query_dict, time_log_output_path):
    # TODO: Duplicate code in nds_power.py. Refactor this part, make it general.
    execution_time_list = []
    total_time_start = time.time()
    if len(query_dict) == 1:
        app_name = "NDS - Data Maintenance - " + list(query_dict.keys())[0]
    else:
        app_name = "NDS - Data Maintenance"
    
    spark_session = SparkSession.builder.appName(
        app_name).getOrCreate()
    spark_app_id = spark_session.sparkContext.applicationId
    DM_start = time.time()
    for query_name, q_content in query_dict.items():
        # show query name in Spark web UI
        spark_session.sparkContext.setJobGroup(query_name, query_name)
        print(f"====== Run {query_name} ======")
        q_report = PysparkBenchReport(spark_session)
        if query_name in DELETE_FUNCS + INVENTORY_DELETE_FUNC:
            summary = q_report.report_on(run_delete_query, spark_session,
                                                           query_name,
                                                           q_content)
        else:
            summary = q_report.report_on(run_insert_query, spark_session,
                                                           q_content)
        print(f"Time taken: {summary['queryTimes']} millis for {query_name}")
        execution_time_list.append((spark_app_id, query_name, summary['queryTimes']))
        q_report.write_summary(query_name, prefix="")
    spark_session.sparkContext.stop()
    DM_end = time.time()
    DM_elapse = DM_end - DM_start
    total_elapse = DM_end - total_time_start
    print("====== Data Maintenance Time: {} s ======".format(DM_elapse))
    print("====== Total Time: {} s ======".format(total_elapse))
    execution_time_list.append(
        (spark_app_id, "Data Maintenance Time", DM_elapse))
    execution_time_list.append(
        (spark_app_id, "Total Time", total_elapse))

    # write to local csv file
    header = ["application_id", "query", "time/s"]
    with open(time_log_output_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(execution_time_list)
    

if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('maintenance_queries_folder',
                        help='folder contains all NDS Data Maintenance queries. If ' +
                        '"--maintenance_queries" is not set, all queries under the folder will be' +
                        'executed.')
    parser.add_argument('time_log',
                        help='path to execution time log, only support local path.',
                        default="")
    parser.add_argument('--maintenance_queries',
                        type=lambda s: s.split(','),
                        help='specify Data Maintenance query names by a comma seprated string.' +
                        ' e.g. "LF_CR,LF_CS"')

    args = parser.parse_args()
    query_dict = get_maintenance_queries(args.maintenance_queries_folder,
                                         args.maintenance_queries)
    run_query(query_dict, args.time_log)
