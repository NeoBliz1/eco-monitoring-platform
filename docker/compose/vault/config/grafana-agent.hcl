vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/grafana_role_id"
      secret_id_file_path = "/tmp/vault_input/grafana_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
GF_SECURITY_ADMIN_USER={{ with secret "secret/data/grafana" }}{{ .Data.data.admin_user }}{{ end }}
GF_SECURITY_ADMIN_PASSWORD={{ with secret "secret/data/grafana" }}{{ .Data.data.admin_password }}{{ end }}
EOH
  destination = "/run/secrets/grafana_creds.env"
  perms       = "0666"
}
