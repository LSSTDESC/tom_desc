FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y && apt-get clean
RUN apt-get install -y postgresql-13 postgresql-client-13 postgresql-13-postgis && apt-get clean

RUN mkdir /secrets
RUN echo "replace_this" >> /secrets/postgres_password

RUN mkdir /build
WORKDIR /build
RUN apt-get -y install git make gcc postgresql-server-dev-13 libssl-dev libreadline-dev zlib1g-dev && \
  git clone https://github.com/segasai/q3c.git && \
  cd q3c && \
  make && \
  make install && \
  cd .. && \
  rm -rf q3c && \
  apt-get purge -y git make gcc postgresql-server-dev-13 libssl-dev libreadline-dev zlib1g-dev && \
  apt-get -y --purge autoremove && \
  apt-get clean

RUN mkdir /var/lib/postgresql/data \
    && chmod 700 /var/lib/postgresql/data \
    && chown postgres:postgres /var/lib/postgresql/data

RUN locale-gen en_US.utf8

RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/13/main/pg_hba.conf
RUN echo "listen_addresses='*'" >> /etc/postgresql/13/main/postgresql.conf
RUN cat /etc/postgresql/13/main/postgresql.conf | \
       perl -pe "s{data_directory *= *'(.*)'(.*)}{data_directory = '/var/lib/postgresql/data'\2}" \
         > /etc/postgresql/13/main/postgresql.conf.new \
    && mv /etc/postgresql/13/main/postgresql.conf /etc/postgresql/13/main/postgresql.conf.old \
    && mv /etc/postgresql/13/main/postgresql.conf.new /etc/postgresql/13/main/postgresql.conf

# Make sure this matches what is in the config file (created just above)
ADD run_postgres.sh /run_postgres.sh

USER postgres
ENV POSTGRES_DATA_DIR=/var/lib/postgresql/data
CMD ["bash", "/run_postgres.sh"]