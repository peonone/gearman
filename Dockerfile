FROM golang:latest AS build

RUN mkdir -p /go/src/github.com/peonone/gearman
COPY . /go/src/github.com/peonone/gearman

WORKDIR /go/src/github.com/peonone/gearman

RUN wget -O /bin/dep https://github.com/golang/dep/releases/download/v0.3.2/dep-linux-amd64 \
 && chmod +x /bin/dep \
 && /bin/dep ensure \
 && CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -ldflags "-extldflags -static" server/gearmand/gearmand.go

FROM alpine:3.6

EXPOSE 4730

VOLUME /data
VOLUME /logs

COPY --from=build /go/src/github.com/peonone/gearman/gearmand /usr/bin
ENTRYPOINT ["/usr/bin/gearmand"]
CMD ["-sql-queue-datasource", "/data/gearmand.dat", "-log-file", "/logs/gearmand.log"]