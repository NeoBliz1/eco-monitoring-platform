vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/redis_role_id"
      secret_id_file_path = "/tmp/vault_input/redis_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
{{ with secret "secret/data/redis" }}{{ .Data.data.redis_password }}{{ end }}
EOH
  destination = "/run/secrets/redis_pass_only"
  perms       = "0666"
}
