FROM ubuntu:16.04


MAINTAINER USGS EROS LSRD http://eros.usgs.gov


LABEL description="(Processing Architecture) Prototype Work Manager"


ENV QPID_PROTON_MIRROR="apache.cs.utah.edu" \
    QPID_PROTON_VERSION="0.20.0"


# Install essential tools
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y build-essential python3 python3-dev python3-pip \
    && apt-get install -y openssl libssl1.0.0 libssl-dev \
    && apt-get install -y swig cmake wget
RUN pip3 install --upgrade pip


COPY qpid-proton-${QPID_PROTON_VERSION}-SHA512-CHECKSUM /usr/src/
RUN cd /usr/src \
    && wget http://${QPID_PROTON_MIRROR}/qpid/proton/${QPID_PROTON_VERSION}/qpid-proton-${QPID_PROTON_VERSION}.tar.gz \
    && sha512sum --check qpid-proton-${QPID_PROTON_VERSION}-SHA512-CHECKSUM \
    && tar -xf qpid-proton-${QPID_PROTON_VERSION}.tar.gz \
    && cd qpid-proton-${QPID_PROTON_VERSION} \
    && mkdir build \
    && cd build \
    && cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DSYSINSTALL_PYTHON=ON \
    && make all \
    && make install \
    && cd \
    && rm -rf /usr/src/*


# Install our application
COPY proto-work-manager.py /bin/proto-work-manager.py
ENTRYPOINT /bin/proto-work-manager.py
