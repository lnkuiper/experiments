docker run --rm --name monetdb-container -e 'MONET_DATABASE=monetdb' -p 50000:50000 -d topaztechnology/monetdb:latest
docker cp .monetdb monetdb-container:/home/monetdb/.monetdb

