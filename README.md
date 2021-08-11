Recipe

docker build -t tom-desc .
docker tag tom-desc registry.nersc.gov/m1727/tom-desc-app
docker push registry.nersc.gov/m1727/tom-desc-app
