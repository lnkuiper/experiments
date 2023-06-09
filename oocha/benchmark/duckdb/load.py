import os
import sys


SYSTEM_DIR = os.path.dirname(__file__)
sys.path.append(f'{SYSTEM_DIR}/..')
from util.util import *


def main():
    con = duckdb.connect(f'{SYSTEM_DIR}/data.db')

    config = load_config(con)
    configs = permute_dicts(config)

    print('Ingesting group data into DuckDB ...')
    for c in tqdm.tqdm(configs):
        name = conf_to_str(c)
        file_name = f'{GROUPS_DATA_DIR}/{name}.csv'

        con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{name}'")
        if con.fetchall()[0][0] == 0:
            con.execute(f"""
                START TRANSACTION;
                CREATE TABLE "{name}" ({', '.join([f'c{cc} {c["type"]}' for cc in range(c['column_count'])])});
                COPY "{name}" FROM '{file_name}';
                COMMIT;
                """)
    print('Done.')


if __name__ == '__main__':
    main()
