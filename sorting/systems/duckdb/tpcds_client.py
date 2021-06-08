import duckdb
import os
import subprocess
import time

def run(con, query_folder, results_folder):
	for qname in os.listdir(query_folder):
		# skip .keep file

		if not qname.endswith('.sql'):
			continue

		# skip if already done
		if (os.path.isfile(results_folder + qname)):
			continue

		# read the query
		with open(query_folder + qname, 'r') as f:
			query = f.read()

		# time and execute the query
		for i in range(5):
			before = time.time()
			con.execute(query)
			after = time.time()

			# write time to csv
			with open(results_folder + 'results.csv', 'a+'):
				print(qname.split('.')[0] + f',{after - before}', file=f)

			con.execute('DROP TABLE output;')

		# create empty file to mark query as done
		open(results_folder + qname, 'w+')

def main():
	for sf in [1, 10, 100, 300]:
		db_name = f'tpcds_sf{sf}.db';
		if (!os.path.isfile(db_name)):
			subprocess.call(f'export SF=sf{sf} && ./load_tpcds.sh', shell=True)
		con = duckdb.connect(db_name)
		run(con, f'../../queries/tpcds/sf{sf}/sql/', f'../../results/duckdb/tpcds/sf{sf}/')

if __name__ == '__main__':
	main()
