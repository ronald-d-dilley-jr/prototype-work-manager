FROM ubuntu:16.04


MAINTAINER USGS EROS LSRD http://eros.usgs.gov


LABEL description="(Processing Architecture) Prototype Work Manager"


# Install essential tools
RUN apt-get update \
    && apt-get install -y build-essential python3 python3-dev  python3-pip
RUN pip3 install --upgrade pip


# Install python libraries we need
RUN pip3 install python-qpid-proton==0.20.0


# Install our application
COPY proto-work-manager.py /bin/proto-work-manager.py
ENTRYPOINT /bin/proto-work-manager.py
