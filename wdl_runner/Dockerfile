FROM phusion/baseimage:0.9.18

RUN sudo apt-get update

# Install Java 8
RUN echo "\n" | sudo add-apt-repository ppa:webupd8team/java
RUN sudo apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN sudo apt-get install oracle-java8-installer -y

# For gcloud
RUN apt-get install python -y
RUN apt-get install curl -y
RUN curl https://sdk.cloud.google.com | bash

# Add the install location explicity to the path so it works for non-interactive shells
ENV PATH /root/google-cloud-sdk/bin:$PATH

# Copy Cromwell and the Cromwell conf template
RUN mkdir /cromwell
COPY cromwell-3eb1623.jar /cromwell/
RUN ln /cromwell/cromwell-3eb1623.jar /cromwell/cromwell.jar
COPY jes_template.conf /cromwell/

# Copy the driver python
COPY cromwell_driver.py /cromwell/
COPY wdl_runner.py /cromwell/

ENV CROMWELL /cromwell/cromwell.jar
ENV CROMWELL_CONF /cromwell/jes_template.conf


# Install pip and requests
RUN curl "https://bootstrap.pypa.io/get-pip.py" | python
RUN pip install requests

WORKDIR /cromwell/