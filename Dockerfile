FROM golang:1.10.0

ENV BASE_DIR /go/src/github.com/anchorfree/data-go

ADD . ${BASE_DIR}

#RUN mkdir -p /var/lib/GeoIP/ \
    #&& curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz | tar xz -C /var/lib/GeoIP \
    #&& curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz | tar xz -C /var/lib/GeoIP \
    #&& mv /var/lib/GeoIP/*/GeoLite2-City.mmdb /var/lib/GeoIP/GeoIP2-City.mmdb \
    #&& mv /var/lib/GeoIP/*/GeoLite2-ASN.mmdb /var/lib/GeoIP/GeoIP2-ISP.mmdb
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
WORKDIR ${BASE_DIR}
RUN dep init && dep ensure
RUN cd ${BASE_DIR} && go test ./...

FROM alpine
RUN touch /test.OK
