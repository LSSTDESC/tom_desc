# Have to build this in context of parent directory, with dockerfile docker_server/Dockerfvile

FROM rknop/devuan-chimaera-rknop

WORKDIR /tom_desc
ENV HOME /

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update && \
   apt-get -y upgrade && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update && \
   apt-get install -y gdal-bin python3 python3-pip librdkafka-dev \
     libpq-dev postgresql-client \
     tmux netcat bind9-dnsutils && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install --upgrade pip

COPY docker_server/pip-packages.txt .
RUN pip install \
	--no-cache \
	--disable-pip-version-check \
	--requirement pip-packages.txt && \
    rm -rf /.cache/pip

RUN mkdir tom_desc
COPY tom_desc/ ./tom_desc/

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

# RUN mkdir -p /.config/hop
# RUN chmod -R 777 /.config/hop
