FROM gcr.io/distroless/static-debian11:nonroot

ENTRYPOINT ["/plaid-cache"]
COPY plaid-cache /
