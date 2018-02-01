FROM python:3

RUN apt-get update && apt-get install -y git
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
RUN rm requirements.txt
RUN git clone https://github.com/icshih/SpiralArm.git
CMD ["/bin/bash"]
