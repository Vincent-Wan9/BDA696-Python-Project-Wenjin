FROM mariadb


WORKDIR /HW6


RUN apt-get update
RUN apt-get install -y mysql-server

COPY ./baseball.sql /HW6/baseball.sql
COPY ./batting_average.sql /HW6/sql_commands.sql
COPY ./docker.sh /HW6/docker.sh


CMD ["/HW6/docker.sh"]

