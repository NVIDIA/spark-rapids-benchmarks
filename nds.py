import argparse
import subprocess
import shutil
import os

def generate_data(args):
    # check if hadoop is installed.
    if (shutil.which('hadoop') is None):
        raise Exception("No Hadoop binary found in current environment, " +
                        "please install Hadoop for data generation in cluster.")
    # submit hadoop MR job to generate data
    os.chdir('tpcds-gen')
    subprocess.run(['hadoop', 'jar', 'target/tpcds-gen-1.0-SNAPSHOT.jar',
        '-d', args.dir, '-p', str(args.parallel), '-s', str(args.scale)])

def generate_query(args):
    pass

def main():
    parser = argparse.ArgumentParser(description='Argument parser for NDS benchmark options.')
    parser.add_argument('--generate', choices=['data', 'query', 'convert'], required=True,
                    help='generate tpc-ds data or queries.')
    parser.add_argument('--dir', required=True,help='target HDFS path for generated data.')
    parser.add_argument('--scale', type=int ,help='volume of data to generate in GB.')
    parser.add_argument('--parallel', type=int ,help='generate data in n parallel MapReduce jobs.')

    args = parser.parse_args()

    if args.generate == 'data':
        generate_data(args)

if __name__ == '__main__':
    main()
