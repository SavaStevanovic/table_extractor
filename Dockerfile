FROM ubuntu:20.04
RUN apt-get update \
  && apt-get install -y -qq --no-install-recommends \
    libglvnd0 \
    libgl1 \
    libglx0 \
    libegl1 \
    libxext6 \
    libx11-6
ENV TZ=Europe/Belgrade
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install -y python3 python3-pip python-opengl python3-tk
COPY ./project/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt
CMD bash
