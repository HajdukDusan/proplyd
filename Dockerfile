FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /proplyd ./cmd/proplyd

FROM gcr.io/distroless/static:nonroot
COPY --from=build /proplyd /proplyd
EXPOSE 7070
USER nonroot:nonroot
ENTRYPOINT ["/proplyd"]
