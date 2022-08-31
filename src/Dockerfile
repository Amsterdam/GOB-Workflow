# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-buster as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-buster as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobworkflow in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Copy gobworkflow module.
COPY gobworkflow gobworkflow

# Copy Alembic config.
COPY alembic alembic
COPY alembic.ini alembic.ini

USER datapunt
# Set GOBOPTIONS=--migrate to run migrations and then exit
CMD python3 -m gobworkflow $GOBOPTIONS


# Test.
FROM application as test
USER root

# Copy test module and tests.
COPY test.sh .flake8 pytest.ini ./
COPY tests tests

# Copy .jenkins scripts to where Jenkins / Ansible expect them.
COPY .jenkins /.jenkins

USER datapunt
