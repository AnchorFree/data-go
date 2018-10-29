FROM golang:1.10.0

ENV BASE_DIR /go/src/github.com/anchorfree/data-go

ADD . ${BASE_DIR}

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
WORKDIR ${BASE_DIR}
RUN dep init && dep ensure
RUN cd ${BASE_DIR} && go test ./...

FROM alpine
RUN touch /test.OK
