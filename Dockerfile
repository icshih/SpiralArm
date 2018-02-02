FROM python:3

#RUN apt-get update && apt-get install -y git && apt-get install -y vim
RUN apt-get update && apt-get install -y vim
RUN mkdir /app
RUN mkdir /app/data
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
RUN rm requirements.txt
COPY ./conf /app/conf
COPY ./py /app/py
COPY ./resources /app/resources
#RUN git clone https://github.com/icshih/SpiralArm.git
#CMD ["python3", "py/test_db_connect.py", "conf/test.conf"]
#CMD ["/bin/bash"]