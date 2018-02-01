FROM python:3

RUN apt-get update && apt-get install -y git && apt-get install -y vim
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
RUN rm requirements.txt
RUN git clone https://github.com/icshih/SpiralArm.git
CMD ["python3", "SpiralArm/py/test_db_connect.py", "SpiralArm/conf/test.conf"]
