#!/bin/bash

cd /tom_desc

# This one should take tens of minutes
echo ""
echo "Running genbrokerstreamgraphs"
echo ""
python manage.py genbrokerstreamgraphs

# This one takes up to a couple of hours
echo ""
echo "Running genbrokercompleteness"
echo ""
python manage.py genbrokercompleteness

# This one takes an hour or more
echo ""
echo "Running genbrokerdelaygraphs"
echo ""
python manage.py genbrokerdelaygraphs
