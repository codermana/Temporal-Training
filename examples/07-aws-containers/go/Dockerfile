# Go Worker image. Multi-stage: compile a static binary, then ship it on a tiny
# base. The result is a few MB — no JVM, no interpreter.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /worker .

FROM gcr.io/distroless/static-debian12
COPY --from=build /worker /worker
# The Worker dials *out* to the Frontend; no inbound port to EXPOSE.
ENTRYPOINT ["/worker"]
