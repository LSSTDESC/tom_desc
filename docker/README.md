# Docker images for DESC Tom

## Postgres image

I build it with

  docker build -t registry.services.nersc.gov/raknop/tom-desc-postgres -f Dockerfile.postgres

## Tom webap image

This docker image was inspired by (but not really copied from)
https://github.com/TOMToolkit/dockertom

The actual TOM toolkit code is NOT included here.  Reason: for the
deploy, I mount a volume from NERSC cfs.  That allows me to edit the
webap without having to redeploy the image to Spin for every edit.

I build it with

  docker build -t registry.services.nersc.gov/raknop/tom-desc-dev .

or

  docker build -t registry.services.nersc.gov/raknop/tom-desc-production .
