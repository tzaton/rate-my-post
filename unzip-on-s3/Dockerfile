# Setup python environment
FROM python:3.7-slim as base
ENV VIRTUAL_ENV=/opt/venv
RUN pip install virtualenv
RUN virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Build final image
FROM python:3.7-slim
COPY --from=base /opt/venv /opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Run job
COPY unzip-on-s3/src /src
WORKDIR /src
ENTRYPOINT ["python", "main.py"]
