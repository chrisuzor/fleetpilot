FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/fleetpilot ./cmd/fleetpilot

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 65532 fleetpilot
COPY --from=build /out/fleetpilot /usr/local/bin/fleetpilot
USER fleetpilot
EXPOSE 8080
ENV PORT=8080
ENTRYPOINT ["/usr/local/bin/fleetpilot"]
