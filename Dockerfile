FROM registry.gitlab.com/datadrivendiscovery/images/primitives:ubuntu-bionic-python36-v2019.6.7
ADD . /d3m-mtl-db-reader
WORKDIR /d3m-mtl-db-reader
RUN pip3 install -r requirements.txt
