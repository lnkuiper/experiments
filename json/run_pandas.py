import os
import pandas as pd
import time


def read_df_from_json(name):
	return pd.read_json(f'data/{name}.json', lines=True)


def main():
	# customer = read_df_from_json('customer')
	lineitem = read_df_from_json('lineitem')
	# nation = read_df_from_json('nation')
	# orders = read_df_from_json('orders')
	# part = read_df_from_json('part')
	# partsupp = read_df_from_json('partsupp')
	# region = read_df_from_json('region')
	# supplier = read_df_from_json('supplier')


if __name__ == '__main__':
	main()
