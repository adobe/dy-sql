ARG python_version

FROM python:${python_version}-alpine

RUN apk add gcc musl-dev libffi-dev openssl-dev git

COPY requirements.txt /tmp/
COPY test_requirements.txt /tmp/
COPY setup.py /tmp/
RUN cd /tmp && pip install -r /tmp/requirements.txt -r /tmp/test_requirements.txt