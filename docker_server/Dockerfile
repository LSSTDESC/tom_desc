# Dockerfile for the DESC TOM
#
# Current "production" versions don't have the code embedded in the dockerfile, but bind-mount
# a directory with a checkout of the appropriate branch of the LSSTDESC/tom_desc git archive
#
# Build one of those in the parent directory to this directory with 
#
#    docker build --target tom-server-bindmount -t <image_name> -f docker_server/Dockerfile .
#
# You can also build a "dev" version, which is used in the tests, with target tom-server-bindmount-dev
#
# TODO: Create a version that has the TOM server code embedded so no bindmounts are needed
#   (except for external data storage).

FROM rknop/devuan-daedalus-rknop AS base

LABEL maintainer="Rob Knop <raknop@lbl.gov>"
SHELL ["/bin/bash", "-c"]
ENV TZ="UTC"

RUN mkdir /tom_desc
ENV HOME="/"

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update \
   && apt-get -y upgrade \
   && apt-get install -y gdal-bin python3 postgresql-client \
                 tmux dumb-init netcat-openbsd bind9-dnsutils curl \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

# Principle of least suprirse
RUN ln -s /usr/bin/python3 /usr/bin/python

# Make a place for astropy to store its state,
#  make sure we can write to it
RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

# Sanity
ENV LESS=-XLRi

WORKDIR /usr/src

# Try to install mongosh
RUN curl -L https://downloads.mongodb.com/compass/mongodb-mongosh-shared-openssl3_2.3.3_amd64.deb -O \
     && dpkg --install mongodb-mongosh-shared-openssl3_2.3.3_amd64.deb \
     && rm -f mongodb-mongosh-shared-openssl3_2.3.3_amd64.deb

# ======================================================================
FROM base AS build

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update \
   && apt-get -y upgrade \
   && apt-get install -y librdkafka-dev cython3 python3-venv python3-pip libpq-dev \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

COPY docker_server/pip-packages.txt .
RUN mkdir /venv
RUN python -mvenv /venv
RUN source /venv/bin/activate && \
    pip -vvv install \
	--no-cache \
	--disable-pip-version-check \
	--requirement pip-packages.txt

COPY docker_server/pubsub.patch /tmp
RUN patch /venv/lib/python3.11/site-packages/pittgoogle/pubsub.py /tmp/pubsub.patch

# ======================================================================
FROM base AS tom-server-bindmount

# Import the pip packages, set the Path so the right python will get run
COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

WORKDIR /tom_desc

# Run the TOM server
EXPOSE 8080
ENTRYPOINT [ "/venv/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

# ======================================================================
# TODO -- make an install here that doesn't copy the git checkout, but
#   exports the git tree to something.  With all the submodules, this
#   could be complicated.

# FROM base AS tom-server
#
# copy --FROM=build /venv/ /venv/
# ENV PATH=/venv/bin:$PATH
# COPY tom_desc/ ./tom_desc/
#
# EXPOSE 8080
# ENTRYPOINT [ "/venv/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]

# ======================================================================
FROM build AS tom-server-bindmount-dev

ENV PATH=/venv/bin:$PATH

WORKDIR /tom_desc

EXPOSE 8080
ENTRYPOINT [ "/venv/bin/gunicorn", "tom_desc.wsgi", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "2"]
