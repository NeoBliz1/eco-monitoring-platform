ui            = true
disable_mlock = true

default_lease_ttl = "768h"
max_lease_ttl     = "768h"

storage "file" {
  path = "/vault/file"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "1"
}
