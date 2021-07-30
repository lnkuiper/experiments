import os
import subprocess
import time
from tqdm import tqdm

def run(query_folder, results_folder, sf):
	for qname in tqdm(os.listdir(query_folder)):
		# skip .keep file
		if not qname.endswith('.sh'):
			continue

		# skip if already done
		if (os.path.isfile(results_folder + qname)):
			continue

		# read the query
		with open(query_folder + qname, 'r') as f:
			query = f.read().replace('$sf$', sf)

		# time and execute the query
		for i in range(5):
			subprocess.run("rm -rf /dev/shm/output.csv", shell=True)

			before = time.time()
			print(f"{query} > /dev/shm/output.csv")
			subprocess.run(f"{query} > /dev/shm/output.csv", shell=True)
			after = time.time()

			# write time to csv
			with open(results_folder + 'results.csv', 'a+') as f:
				print(qname.split('.')[0] + f',{after - before}', file=f)

		# create empty file to mark query as done
		open(results_folder + qname, 'w+')

def main():
    sf = os.environ['SF']
    run('../../queries/tpcds/catalog_sales/gnu/', f'../../results/gnu/tpcds/sf{sf}/catalog_sales/', sf)
    run('../../queries/tpcds/customer/gnu/', f'../../results/gnu/tpcds/sf{sf}/customer/', sf)

if __name__ == '__main__':
	main()
