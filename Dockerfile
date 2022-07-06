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
   apt-get install -y gdal-bin python3 python3-pip librdkafka-dev tmux libpq-dev postgresql-client && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install \
	--no-cache \
	--disable-pip-version-check \
	--requirement requirements.txt && \
    rm -rf /.cache/pip

# Will mount /tom_desc from a host directory
# COPY . .

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

# RUN mkdir -p /.config/hop
# RUN chmod -R 777 /.config/hop
