FROM python:3.7

WORKDIR /tom_desc

COPY requirements.txt .

RUN pip install \
	--no-cache \
	--disable-pip-version-check \
	--requirement requirements.txt

COPY . .

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

#CMD [ \
#	"gunicorn", \
#	"--bind=0.0.0.0:8080", \
#	"--worker-class=gevent", \
#	"--workers=4", \
#	"--timeout=300", \
#	"--access-logfile=-", \
#	"--error-logfile=-", \
#	"tom_desc.wsgi:application" \
#	]

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy
