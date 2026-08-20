vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/vector_role_id"
      secret_id_file_path = "/tmp/vault_input/vector_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
KAFKA_USER="{{ with secret "secret/data/kafka" }}{{ .Data.data.vector_user }}{{ end }}"
KAFKA_PASSWORD="{{ with secret "secret/data/kafka" }}{{ .Data.data.vector_pass }}{{ end }}"
CH_USER="{{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_user }}{{ end }}"
CH_PASSWORD="{{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_password }}{{ end }}"
CH_DB="{{ with secret "secret/data/clickhouse" }}{{ .Data.data.clickhouse_db }}{{ end }}"
EOH
  destination = "/run/secrets/vector_creds.env"
  perms       = "0666"
}
