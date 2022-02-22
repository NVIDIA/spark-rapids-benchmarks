import argparse
import subprocess
import shutil
import sys
import os
from tabnanny import check


def check_build():
    # Check if necessary executable or jars are built.
    if not (os.path.exists('tpcds-gen/target/tpcds-gen-1.0-SNAPSHOT.jar') and
        os.path.exists('tpcds-gen/target/tools/dsdgen')):
        raise Exception('Target jar file is not found in `target` folder, ' +
                        'please refer to README document and build this project first.')


def generate_data(args):
    # Check if hadoop is installed.
    if shutil.which('hadoop') is None:
        raise Exception('No Hadoop binary found in current environment, ' +
                        'please install Hadoop for data generation in cluster.')
    check_build()
    # Submit hadoop MR job to generate data
    os.chdir('tpcds-gen')
    subprocess.run(['hadoop', 'jar', 'target/tpcds-gen-1.0-SNAPSHOT.jar',
                    '-d', args.dir, '-p', args.parallel, '-s', args.scale], check=True)


def generate_query(args):
    check_build()
    # copy tpcds.idx to working dir, it's required by TPCDS tool
    subprocess.run(['cp', './tpcds-gen/target/tools/tpcds.idx', './tpcds.idx'],check=True)

    if not os.path.isdir(args.query_output_dir):
        os.makedirs(args.query_output_dir)
    subprocess.run(['./tpcds-gen/target/tools/dsqgen', '-template', args.template, '-directory',
        args.template_dir, '-dialect', 'spark', '-scale', args.scale, '-output_dir',
        args.query_output_dir],check=True)
    # remove it after use.
    subprocess.run(['rm', './tpcds.idx'], check=True)

def generate_query_streams(args):
    check_build()
    # Copy tpcds.idx to working dir, it's required by TPCDS tool.
    subprocess.run(['cp', './tpcds-gen/target/tools/tpcds.idx', './tpcds.idx'],check=True)

    if not os.path.isdir(args.query_output_dir):
        os.makedirs(args.query_output_dir)

    subprocess.run(['./tpcds-gen/target/tools/dsqgen', '-scale', args.scale, '-directory',
        args.template_dir, '-output_dir', args.query_output_dir, '-input',
        './query_templates_nds/templates.lst', '-dialect', 'spark', '-streams', args.streams],
        check=True)
    # Remove it after use.
    subprocess.run(['rm', './tpcds.idx'], check=True)

def convert_csv_to_parquet(args):
    # This will submit a Spark job to read the TPCDS raw data (csv with "|" delimiter) then save as Parquet files.
    # The configuration for this will be read from an external template file. User could set Spark parameters there.
    with open(args.spark_submit_template, 'r') as f:
        template = f.read()
    
    
    subprocess.run([])
    pass

def main():
    parser = argparse.ArgumentParser(
        description='Argument parser for NDS benchmark options.')
    parser.add_argument('--generate', choices=['data', 'query', 'streams', 'convert'], required=True,
        help='generate tpc-ds data or queries.')
    parser.add_argument('--data-dir', help='If generating data: target HDFS path for generated data.')
    parser.add_argument('--template-dir', help='directory to find query templates.')
    parser.add_argument('--scale', help='volume of data to generate in GB.')
    parser.add_argument('--parallel', help='generate data in n parallel MapReduce jobs.')
    parser.add_argument('--template', required='query' in sys.argv,
        help='query template used to build queries.')
    parser.add_argument('--streams', help='generate how many query streams.')
    parser.add_argument('--query-output-dir', help='directory to write query streams.')
    parser.add_argument('--spark-submit-template', help='A Spark config template contains necessary Spark job configurations.')
    args = parser.parse_args()

    if args.generate == 'data':
        generate_data(args)

    if args.generate == 'query':
        generate_query(args)

    if args.generate == 'streams':
        generate_query_streams(args)
    
    if args.generate == 'convert':
        convert


if __name__ == '__main__':
    main()
