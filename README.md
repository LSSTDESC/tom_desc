## Installing the TOM Toolkit locally

The toolkit github repository is at https://github.com/LSSTDESC/tom_desc.  Install it locally.

The use of a virutal environment is recommended.

```bash
python3 -m venv tom_env/
```
Now that we have created the virtual environment, we can activate it:
```bash
source tom_env/bin/activate
```

Install the requisite packages into the virtual environment

```bash
pip install -r requirements.txt
```

## Local Database Server

Getting a dockerized  database up and running is a pre-requisite to all of the local development methods. Here's how:
```bash
export DB_HOST=127.0.0.1

docker run --name tom-desc-postgres -v /var/lib/postgresql/data -p 5432:5432 -d postgres:11.1

docker exec -it tom-desc-postgres /bin/bash  # start a shell inside the postgres container

createdb -U postgres tom_desc                # create the tom_demo database
exit                                         # leave the container, back to your shell
```

If this is your first time creating the `tom_desc` database, you must create the tables and put
some data in the database that you just created.
```bash
# make sure you are in your virtual environment, then
./manage.py migrate           # create the tables
./manage.py collectstatic     # gather up the static files for serving
```
#

## Running the TOM
Now that you have a database server up and running on your local machine, consider these alternatives for local development your TOM:

### Aternative 1: Running `tom-desc` in your virtual environment, via `./manage.py runserver`
<details>

```bash
./manage.py runserver &
# see the output "Starting development server at <URL>" for where to point your browser.
```
</details>

### Alternative 2: Running `tom-desc` dockerized, via `docker run`
<details>

```bash
docker build -t tom-desc .
```

According to TOM instructions this works but it didn't on my Mac.
```bash
docker build -t tom-desc .                     # build a docker image of your current sandbox
docker run --network="host" tom-desc &
# point your browser at localhost 
```

To get it working on my Mac I had to do the following
```bash
docker network create tom-net
docker network connect tom-net tom-desc-postgres
docker run -p 8080:8080 --network=tom-net tom-desc &
# point your browser at localhost:8080
```
</details>

## Send to NERSC
```bash
docker tag tom-desc registry.nersc.gov/m1727/tom-desc-app
docker push registry.nersc.gov/m1727/tom-desc-app
```
