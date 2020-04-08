FROM ubuntu:latest
RUN  mkdir /src
COPY . /src
run apt-get update 
run apt-get upgrade -y
RUN apt-get update && apt-get install -y python-pip
RUN pip install numpy
RUN pip install pandas
RUN pip install Cython
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
RUN pip install sklearn
CMD ["python","/src/KYC_facechart.py"]