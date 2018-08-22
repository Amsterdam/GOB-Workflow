FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Copy .jenkins scripts to where jenkins / ansible expect them
COPY .jenkins /.jenkins

# Install gobworkflow in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobworkflow module
COPY gobworkflow gobworkflow

CMD ['python3', '- m', 'gobworkflow']