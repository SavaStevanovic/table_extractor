docker build -t table_extractor .
docker run --rm --name table_extractor -dit -v `pwd`/project:/app table_extractor
