vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/clickhouse_role_id"
      secret_id_file_path = "/tmp/vault_input/clickhouse_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
CLICKHOUSE_DB={{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_db }}{{ end }}
CLICKHOUSE_USER={{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_user }}{{ end }}
CLICKHOUSE_PASSWORD={{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_password }}{{ end }}
CLICKHOUSE_SHA256_PASSWORD={{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_sha256_password }}{{ end }}
CLICKHOUSE_SHA256_DEFAULT_PASSWORD={{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_sha256_default_password }}{{ end }}
EOH
  destination = "/run/secrets/clickhouse_creds.env"
  perms       = "0666"
}
