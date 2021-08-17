FROM python:3.8
RUN pip install --upgrade pip

WORKDIR /tom_desc

COPY requirements.txt .

RUN apt-get update &&\
   apt-get install -y gdal-bin

RUN pip install \
	--no-cache \
	--disable-pip-version-check \
	--requirement requirements.txt

COPY . .

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

# RUN mkdir -p /.config/hop
# RUN chmod -R 777 /.config/hop
