#!/bin/bash
python manage.py collectstatic --noinput
python manage.py migrate
python manage.py createsuperuser --noinput --username root --email root@example.com
python manage.py runserver 0.0.0.0:8080
echo "Listening on 0.0.0.0:8080"
