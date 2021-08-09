FROM python:3.7

WORKDIR /tom_desc

COPY requirements.txt .

RUN pip install \
	--no-cache \
	--disable-pip-version-check \
	--requirement requirements.txt

COPY . .

CMD [ \
	"gunicorn", \
	"--bind=0.0.0.0:8080", \
	"--worker-class=gevent", \
	"--workers=4", \
	"--timeout=300", \
	"--access-logfile=-", \
	"--error-logfile=-", \
	"tom_desc.wsgi:application" \
	]
