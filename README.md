# web-browsing

## Docker

### step 1 - build Image using Dockerfile
docker build -t web-browsing-image:1.0 .

### step 2 - create container with mounted directory (using this web-browsing directory)
docker run -it --name web-browsing-container -v "$(Get-Location):/app" web-browsing-image:1.0 bash

exit

### step 3 - access to Contatiner and create Enviroment "web-browsing-container"
docker start -ai web-browsing-container


### step 4 - Python libraries intallation

####  step 4.1  - create environment
python -m venv .venv

####  step 4.2  - access to environment
source .venv/bin/activate

####  step 4.3  - update pip
python -m pip install --upgrade pip

####  step 4.2  - install requirements
pip install -r requirements.txt