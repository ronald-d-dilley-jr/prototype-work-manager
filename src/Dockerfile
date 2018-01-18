FROM alpine


MAINTAINER USGS EROS LSRD http://eros.usgs.gov


LABEL description="(Processing Architecture) Prototype Work Manager"


# Update the OS packages to the latest versions
RUN apk update --no-cache


# Install packages we want to use
RUN apk add --no-cache bash python3
RUN hash -r


# Install python libraries we need
RUN pip3 install pika


COPY proto-work-manager.py /bin/proto-work-manager.py
ENTRYPOINT /bin/proto-work-manager.py
