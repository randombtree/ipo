# ICON sample application container
FROM icon/bullseye

ARG PACKET
COPY ./dist/$PACKET /tmp

RUN tar -tzf /tmp/$PACKET && pip install /tmp/$PACKET
CMD ["python3", "-m", "iconsrv"]
