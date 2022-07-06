FROM python:3.9.13

WORKDIR /tom_desc
ENV HOME /

RUN DEBIAN_FRONTEND="noninteractive" \
   apt-get update && \
   apt-get -y upgrade && \
   apt-get -y install postgresql-client librdkafka-dev && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache --disable-pip-version-check \
   asdf==2.12.0 \
   asn1crypto==1.5.1 \
   astroplan==0.8 \
   astropy==5.1 \
   astroquery==0.4.6 \
   attrs==21.4.0 \
   beautifulsoup4==4.11.1 \
   certifi==2022.6.15 \
   cffi==1.15.1 \
   chardet==5.0.0 \
   cryptography==37.0.4 \
   cycler==0.11.0 \
   Django==4.0.6 \
   django-bootstrap4==22.1 \
   django-contrib-comments==2.2.0 \
   django-crispy-forms==1.14.0 \
   django-extensions==3.1.5 \
   django-filter==22.1 \
   django-gravatar2==1.4.4 \
   django-guardian==2.4.0 \
   entrypoints==0.4 \
   fits2image==0.4.6 \
   gevent==21.12.0 \
   greenlet==1.1.2 \
   gunicorn==20.1.0 \
   gwcs==0.18.1 \
   html5lib==1.1 \
   idna==3.3 \
   jeepney==0.8.0 \
   jsonschema==4.6.1 \
   keyring==23.6.0 \
   kiwisolver==1.4.3 \
   matplotlib==3.5.2 \
   numpy==1.23.0 \
   Pillow==9.2.0 \
   plotly==5.9.0 \
   pycparser==2.21 \
   pyparsing==3.0.9 \
   pyrsistent==0.18.1 \
   python-dateutil==2.8.2 \
   pytz==2022.1 \
   PyYAML==6.0 \
   requests==2.28.1 \
   retrying==1.3.3 \
   scipy==1.8.1 \
   SecretStorage==3.3.2 \
   semantic-version==2.10.0 \
   six==1.16.0 \
   soupsieve==2.3.2.post1 \
   specutils==1.7.0 \
   sqlparse==0.4.2 \
   urllib3==1.26.9 \
   webencodings==0.5.1 \
   whitenoise==6.2.0

RUN mkdir -p /.astropy
RUN chmod -R 777 /.astropy

EXPOSE 8080
ENTRYPOINT [ \
        "gunicorn", \
        "--bind=0.0.0.0:8080", \
        "--worker-class=gevent", \
        "--workers=4", \
        "--timeout=300", \
        "--access-logfile=-", \
        "--error-logfile=-", \
        "tom_desc.wsgi:application" \
        ]

