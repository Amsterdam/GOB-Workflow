FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

# Copy .jenkins scripts to where jenkins / ansible expect them
COPY .jenkins /.jenkins

# Install gobworkflow in /app folder
WORKDIR /app

# Install SQL Server driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install GDAL
RUN apt-get install -y libgdal-dev

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobworkflow module
COPY gobworkflow gobworkflow

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

# Set GOBOPTIONS=--migrate to run migrations and then exit
CMD python3 -m gobworkflow $GOBOPTIONS
