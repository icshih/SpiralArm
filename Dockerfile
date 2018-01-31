FROM python:3

RUN apt-get update && apt-get install -y git

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN git clone https://github.com/icshih/SpiralArm.git sa

CMD ["/bin/bash"]
