FROM registry.docker.dev.sogou-inc.com:5000/clouddev/bigdatakit:1.1.0

ENV APPROOT /search/beaver
WORKDIR $APPROOT
ADD .tmp $APPROOT

RUN mkdir -p logs
CMD bin/start.sh >logs/beaver.out 2>&1