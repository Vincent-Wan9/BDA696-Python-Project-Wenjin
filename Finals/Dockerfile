FROM ubuntu

RUN apt-get update \
  && apt-get install --yes \
     build-essential \
     libmysqlclient-dev \
     mysql-client \
     python3 \
     python3-pip \
     python3-dev \
     python3-pymysql \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir Finals
WORKDIR /Finals

# Copy over database
COPY ./baseball.sql /Finals/baseball.sql

#Copy over code
COPY ./Assignment4.py /Finals/Assignment4.py
COPY ./cat_correlation.py /Finals/cat_correlation.py
COPY ./midterm.py /Finals/midterm.py
COPY ./Examine_feature.py /Finals/Examine_feature.py
COPY ./ML_Model_60_day.py /Finals/ML_Model_60_day.py

COPY ./code.sh /Finals/code.sh
COPY ./features.sql /Finals/features.sql

# Get necessary python libraries
COPY ./requirements.txt /Finals/requirements.txt
RUN pip3 install --compile --no-cache-dir -r /Finals/requirements.txt

RUN chmod +x /Finals/code.sh

CMD ["/Finals/code.sh"]
