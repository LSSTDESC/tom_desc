# Have to build this in context of parent directory, with dockerfile docker_server/Dockerfvile

FROM rknop/devuan-chimaera-rknop AS base

RUN mkdir /tom_desc
WORKDIR /tom_desc
ENV HOME /

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update && \
   apt-get -y upgrade && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# I haven't successfully installed all the pip packages with pypy3
# pypy3 pypy3-dev pypy3-venv gcc g++ gfortran libopenblas-dev pkg-config \
RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update && \
   apt-get install -y gdal-bin librdkafka-dev cython3 python3 python3-pip \
     libpq-dev postgresql-client \
     tmux dumb-init netcat-openbsd bind9-dnsutils && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
# RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED
# RUN pypy3 -m venv /venv
# ENV PATH="/venv/bin:$PATH"

COPY docker_server/pip-packages.txt .
RUN pip -vvv install \
	--no-cache \
	--disable-pip-version-check \
	--requirement pip-packages.txt && \
    rm -rf /.cache/pip

COPY docker_server/pubsub.patch /tmp
RUN patch /usr/local/lib/python3.9/dist-packages/pittgoogle/pubsub.py /tmp/pubsub.patch

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

ENV LESS=-XLRi

# ======================================================================

FROM base AS tom-server-bindmount

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

# ======================================================================

FROM base AS tom-server

COPY tom_desc/ ./tom_desc/

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

# ======================================================================

FROM base AS tom-server-extcode

RUN DEBIAN_FRONTEND="noninteractive" \
  apt-get update && \
  apt-get install -y python3-pytest && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/pytest-3 /usr/bin/pytest

RUN pip install --no-cache --disable-pip-version-check pytest-timestamper

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]
