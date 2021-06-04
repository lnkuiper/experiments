from clickhouse_driver import Client
client = Client(host = 'clickhouse-server',
                port = '9000',
                database='db_name')

print(client.execute('SELECT 1'))
