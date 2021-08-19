# ICON container base image
FROM python:bullseye

COPY ./dist/ipo-0.0.1.tar.gz /tmp

RUN tar -tzf /tmp/ipo-0.0.1.tar.gz && pip install /tmp/ipo-0.0.1.tar.gz
CMD ["python3"]
