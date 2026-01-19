FROM redis:6.2-alpine
#redis:6.0.13
# Copy the certificates to the container
COPY ./tls /tls
# Start Redis with TLS enabled
CMD ["redis-server", "--tls-port", "6379", "--port", "0", "--tls-cert-file", "/tls/redis.crt", "--tls-key-file", "/tls/redis.key", "--tls-ca-cert-file", "/tls/ca.crt", "--tls-auth-clients", "yes"]