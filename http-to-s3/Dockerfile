# Setup python environment
FROM python:3.7.6
ENV VIRTUAL_ENV=/opt/venv
RUN pip install virtualenv
RUN virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run job
COPY http_to_s3.py run_etl.py /src/
WORKDIR /src
ENTRYPOINT ["python", "run_etl.py"]
