## Local setup

The toolkit github repository is at https://github.com/LSSTDESC/tom_desc

TOM Postgres recommends the use of a virutal environment.

TOM Toolkit and enter that directory

```bash
pip install -r requirements.txt
```

## Local Database Server
export DB_HOST=127.0.0.1


docker run --name tom-desc-postgres -v /var/lib/postgresql/data -p 5432:5432 -d postgres:11.1

docker exec -it tom-desc-postgres /bin/bash  # start a shell inside the postgres container

createdb -U postgres tom_desc                # create the tom_demo database
exit                                         # leave the container, back to your shell

If creating the database for the first time

```bash
# make sure you are in your virtual environment, then
./manage.py migrate           # create the tables
./manage.py collectstatic     # gather up the static files for serving
```

## Local Docker Recipe

```bash
docker build -t tom-desc .
```

## Send to NERSC
```bash
docker tag tom-desc registry.nersc.gov/m1727/tom-desc-app
docker push registry.nersc.gov/m1727/tom-desc-app
```
