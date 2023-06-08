import os
import shutil


DATA_DIR = 'data'
RESULTS_DIR = 'results'


def main():
	for d in [DATA_DIR, RESULTS_DIR]:
		if os.path.exists(d):
			shutil.rmtree(d)


if __name__ == '__main__':
	main()
