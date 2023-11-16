FROM rknop/devuan-daedalus-rknop AS base
MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir /build
WORKDIR /build

# Do the apt-get stuff all in one phase to avoid cluttering with
# temporary files.  But, also, I need to compile q3c, so I temporariliy
# install some dev stuff that I then purge... all in one big
# RUN.
RUN apt-get update && apt-get -y upgrade && \
  apt-get -y install \
    make git gcc postgresql-15 postgresql-client-15 postgresql-15-postgis \
    postgresql-server-dev-15 pgtop libssl-dev libreadline-dev zlib1g-dev tmux netcat-openbsd \
    libzstd1 liblz4-1 libzstd-dev liblz4-dev curl && \
  git clone https://github.com/segasai/q3c.git && \
  cd q3c && \
  make && \
  make install && \
  cd .. && \
  rm -rf q3c && \
  curl -L https://github.com/ossc-db/pg_hint_plan/archive/refs/tags/REL15_1_5_1.tar.gz \
       -o pg_hint_plan-REL15_1_5_1.tar.gz && \
  tar -xpf pg_hint_plan-REL15_1_5_1.tar.gz && \
  cd pg_hint_plan-REL15_1_5_1 && \
  make && \
  make install && \
  cd .. && \
  rm -rf pg_hint_plan-REL15_1_5_1* && \
  apt-get purge -y git make gcc postgresql-server-dev-15 libssl-dev libreadline-dev zlib1g-dev \
                   libzstd-dev liblz4-dev curl && \
  apt-get -y --purge autoremove && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN mkdir /var/lib/postgresql/data \
    && chmod 700 /var/lib/postgresql/data \
    && chown postgres:postgres /var/lib/postgresql/data

RUN locale-gen en_US.utf8

RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/15/main/pg_hba.conf
COPY postgresql.conf /etc/postgresql/15/main/postgresql.conf

ENV POSTGRES_DATA_DIR=/var/lib/postgresql/data
ENV LESS=-XLRi

# ======================================================================

FROM base AS tom-postgres

# Make sure this matches what is in the config file (created just above)
# (There is some futzing about here to make sure the right permissions are
# on the file regardless of the permissions as it exists on the machine
# where the docker image is built.  I don't know why a simple ADD
# followed by CHMOD didn't work.)
COPY run_postgres.sh /tmp/run_postgres.sh
RUN cp /tmp/run_postgres.sh /run_postgres.sh && chmod 755 /run_postgres.sh

USER postgres
CMD ["bash", "/run_postgres.sh"]

# ======================================================================

FROM base AS tom-pgdump

RUN mkdir /home/pgdump
ENV HOME /home/pgdump
ADD run_pgdump.sh /home/pgdump/run_pgdump.sh

USER postgres
ENTRYPOINT [ "/bin/bash", "/home/pgdump/run_pgdump.sh" ]
