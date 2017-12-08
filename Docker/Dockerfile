# SGE container
# for testing apps that interface with SGE
# designed to mimic SGE installation on phoenix HPC cluster at NYULMC

FROM debian:wheezy

MAINTAINER Stephen M. Kelly

# user account to use with SGE
ENV sge_user="sge001"

# make sure the package repository is up to date
RUN DEBIAN_FRONTEND=noninteractive apt-get -qq update

# install required dependencies
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y gridengine-master gridengine-exec gridengine-common gridengine-qmon gridengine-client
RUN DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y sudo
RUN apt-get install git -y
RUN apt-get -y install python
# RUN apt-get -y install python-virtualenv python-pip
RUN apt-get install wget -y
RUN wget https://bootstrap.pypa.io/get-pip.py --no-check-certificate
RUN python get-pip.py


# setup 'util' repo
RUN git clone https://github.com/NYU-Molecular-Pathology/util.git
RUN cd util/ && pip install -r requirements.txt

# setup 'sge001' user for
RUN useradd $sge_user --create-home
RUN adduser $sge_user sudo

# setup host information
ADD exec_host /exec_host

# Set maximum of available memory - this is avaiable memory - 1GB
RUN sed -i "s|complex_values        NONE|complex_values        h_vmem=`grep 'MemTotal' /proc/meminfo | awk '{print ($2 - 1000000)}'`k|g" /exec_host

# add hosts data
ADD host_group_entry /host_group_entry
ADD queue /queue


USER root
# add job test script
ADD test.sh /test.sh
RUN chmod +x /test.sh
# add startup script
ADD run.sh /root/run.sh
RUN chmod +x /root/run.sh

# Container Startup Commands; configure SGE with hostname & useraccount at runtime
CMD bash -C '/root/run.sh' "$sge_user"; bash
