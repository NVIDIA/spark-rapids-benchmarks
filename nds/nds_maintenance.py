import argparse
import csv
import time

from pyspark.sql import SparkSession
from PysparkBenchReport import PysparkBenchReport

from check import get_abs_path

def get_maintenance_queries(folder, spec_queries):
    """get query content from DM query files

    Args:
        folder (str): folder to Data Maintenance query files
        spec_queries (list[str]): specific target Data Maintenance queries
    Returns:
        dict{str: list[str]}: a dict contains Data Maintenance query name and its content.
    """
    #TODO: Add delete functions
    DM_FUNCS = ['LF_CR',
                'LF_CS',
                'LF_I',
                'LF_SR',
                'LF_SS',
                'LF_WR',
                'LF_WS']
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
            q_content = [ c + ';' for c in f.read().split(';')[:-1]]
            q_dict[q] = q_content
    return q_dict

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
        for q in q_content:
            summary = q_report.report_on(spark_session.sql,
                                                        q)
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
