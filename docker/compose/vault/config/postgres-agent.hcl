vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/postgres_role_id"
      secret_id_file_path = "/tmp/vault_input/postgres_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
ECO_USER_NAME={{ with secret "secret/data/postgres" }}{{ .Data.data.eco_user_name }}{{ end }}
POSTGRES_PASSWORD={{ with secret "secret/data/postgres" }}{{ .Data.data.postgres_password }}{{ end }}
EXPORTER_DB_USERNAME={{ with secret "secret/data/postgres" }}{{ .Data.data.metrics_user_name }}{{ end }}
EXPORTER_DB_PASSWORD={{ with secret "secret/data/postgres" }}{{ .Data.data.metrics_user_password }}{{ end }}
ECO_USER_PASSWORD={{ with secret "secret/data/postgres" }}{{ .Data.data.eco_user_password }}{{ end }}
EOH
  destination = "/run/secrets/postgres_creds.env"
  perms       = "0444"
}
