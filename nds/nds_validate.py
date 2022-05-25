import argparse
import math
import time
from typing import Iterable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

from nds_power import gen_sql_from_stream

def compare_results(spark_session: SparkSession,
                    input1: str,
                    input2: str,
                    input_format: str,
                    ignore_ordering: bool,
                    use_iterator=False,
                    max_errors=10,
                    epsilon=0.00001):
    """_summary_

    Args:
        spark_session (SparkSession): Spark Session to hold the comparison
        input1 (str): path for the first input data
        input2 (str): path for the second input data
        input_format (str): data source format, e.g. parquet, orc
        ignore_ordering (bool): whether ignoring the order of input data.
            If true, we will order by ourselves.
        use_iterator (bool, optional): When set to true, use `toLocalIterator` to load one partition
            at a time into driver memory, reducing memory usage at the cost of performance because
            processing will be single-threaded. Defaults to False.
        max_errors (int, optional): Maximum number of differences to report. Defaults to 10.
        epsilon (float, optional): Allow for differences in precision when comparing floating point
            values. Defaults to 0.00001.

    Returns:
        Iterable[Row]: _description_
    """
    df1 = spark_session.read.format(input_format).load(input1)
    df2 = spark_session.read.format(input_format).load(input2)
    count1 = df1.count()
    count2 = df2.count()

    if(count1 == count2):
        #TODO: need partitioned collect for NDS? there's no partitioned output currently
        result1 = collect_results(df1, ignore_ordering, use_iterator)
        result2 = collect_results(df2, ignore_ordering, use_iterator)

        errors = 0
        i = 0
        while i < count1 and errors < max_errors:
            lhs = next(result1)
            rhs = next(result2)
            if not rowEqual(list(lhs), list(rhs), epsilon):
                print(f"Row {i}: \n{list(lhs)}\n{list(rhs)}\n")
                errors += 1
            i += 1
        print(f"Processed {i} rows")
        
        if errors == max_errors:
            print(f"Aborting comparison after reaching maximum of {max_errors} errors")
        elif errors == 0:
            print("Results match")
        else:
            print(f"There were {errors} errors")
    else:
        print(f"DataFrame row counts do not match: {count1} != {count2}")

def collect_results(df: DataFrame,
                   ignore_ordering: bool,
                   use_iterator: bool):
    # apply sorting if specified
    non_float_cols = [col(field.name) for \
        field in df.schema.fields \
            if (field.dataType.typeName() != FloatType.typeName()) \
                and \
                (field.dataType.typeName() != DoubleType.typeName())]
    float_cols = [col(field.name) for \
        field in df.schema.fields \
            if (field.dataType.typeName() == FloatType.typeName()) \
                or \
                (field.dataType.typeName() == DoubleType.typeName())]
    if ignore_ordering:
        df = df.sort(non_float_cols + float_cols)

    # TODO: do we still need this for NDS? Query outputs are usually 1 - 100 rows,
    #       there should'nt be memory pressure.
    if use_iterator:
        it = df.toLocalIterator()
    else:
        print("Collecting rows from DataFrame")
        t1 = time.time()
        rows = df.collect()
        t2 = time.time()
        print(f"Collected {len(rows)} rows in {t2-t1} seconds")
        it = iter(rows)
    return it

def rowEqual(row1, row2, epsilon):
    # only simple types in a row for NDS results
    return all([compare(lhs, rhs, epsilon) for lhs, rhs in zip(row1, row2)])

def compare(expected, actual, epsilon=0.00001):
    #TODO 1: we can optimize this with case-match after Python 3.10
    #TODO 2: we can support complex data types like nested type if needed in the future.
    #        now NDS only contains simple data types.
    if isinstance(expected, float) and isinstance(actual, float):
        # Double is converted to float in pyspark...
        if math.isnan(expected) and math.isnan(actual):
            return True
        else:
            return math.isclose(expected, actual, rel_tol=epsilon)
    elif isinstance(expected, str) and isinstance(actual, str):
        return expected == actual
    elif expected == None and actual == None:
        return True
    elif expected != None and actual == None:
        return False
    elif expected == None and actual != None:
        return False
    else:
        return expected == actual

def iterate_queries(spark_session: SparkSession,
                    input1: str,
                    input2: str,
                    input_format: str,
                    ignore_ordering: bool,
                    queries: list,
                    use_iterator=False,
                    max_errors=10,
                    epsilon=0.00001):
    # Iterate each query folder for a Power Run output
    # Providing a list instead of hard-coding all NDS queires is to satify the arbitary queries run.
    for query in queries:
        sub_input1 = input1 + '/' + query
        sub_input2 = input2 + '/' + query
        print(f"=== Comparing Query: {query} ===")
        compare_results(spark_session,
                        sub_input1,
                        sub_input2,
                        input_format,
                        ignore_ordering,
                        use_iterator=use_iterator,
                        max_errors=max_errors,
                        epsilon=epsilon)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('input1',
                        help='path of the first input data')
    parser.add_argument('input2',
                        help='path of the second input data')
    parser.add_argument('input_format',
                        help='data source type. e.g. parquet, orc')
    parser.add_argument('query_stream_file',
                        help='query stream file that contains NDS queries in specific order.')
    parser.add_argument('--max_errors',
                        help='Maximum number of differences to report.',
                        type=int,
                        default=10)
    parser.add_argument('--epsilon',
                        type=float,
                        default=0.00001,
                        help='Allow for differences in precision when comparing floating point values.')
    parser.add_argument('--ignore_ordering',
                        action='store_true',
                        help='Sort the data collected from the DataFrames before comparing them.')
    parser.add_argument('--use_iterator',
                        action='store_true',
                        help='When set, use `toLocalIterator` to load one partition at a' +
                        'time into driver memory, reducing memory usage at the cost of performance' +
                        'because processing will be single-threaded.')
    args = parser.parse_args()
    query_dict = gen_sql_from_stream(args.query_stream_file)
    session_builder = SparkSession.builder.appName("Validate Query Output").getOrCreate()
    iterate_queries(session_builder,
                    args.input1,
                    args.input2,
                    args.input_format,
                    args.ignore_ordering,
                    query_dict.keys(),
                    use_iterator=args.use_iterator,
                    max_errors=args.max_errors,
                    epsilon=args.epsilon)