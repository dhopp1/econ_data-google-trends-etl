#start with ubuntu as base
FROM ubuntu:latest

#install python and git
RUN apt-get update && apt-get install -y \
	python3 \
	python3-pip \
	git \
	python3-psycopg2 \
	apt-utils && \
	#install necessary python packages
	pip3 install pandas && \
	pip3 install googletrans && \
	pip3 install pytrends && \
	pip3 install psycopg2 && \
	pip3 install sqlalchemy && \
	git clone https://github.com/dhopp1/econ_data-google-trends-etl.git

#keep running
CMD tail -f /dev/null