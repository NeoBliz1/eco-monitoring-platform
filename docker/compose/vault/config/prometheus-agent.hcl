vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/prometheus_role_id"
      secret_id_file_path = "/tmp/vault_input/prometheus_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
basic_auth_users:
  {{ with secret "secret/data/prometheus" }}{{ .Data.data.basic_auth_users }}{{ end }}
EOH
  destination = "/run/secrets/prometheus_web_config"
  perms       = "0440"
}

template {
  contents = <<EOH
basic_auth:
  username: "{{ with secret "secret/data/prometheus" }}{{ .Data.data.user }}{{ end }}"
  password: "{{ with secret "secret/data/prometheus" }}{{ .Data.data.password }}{{ end }}"
EOH
  destination = "/run/secrets/promtool_http_config.yml"
  perms       = "0440"
}

template {
  contents = <<EOH
{{ with secret "secret/data/prometheus" }}{{ .Data.data.user }}{{ end }}
EOH
  destination = "/run/secrets/prom_exporter_username"
  perms       = "0440"
}

template {
  contents = <<EOH
{{ with secret "secret/data/prometheus" }}{{ .Data.data.password }}{{ end }}
EOH
  destination = "/run/secrets/prom_exporter_password"
  perms       = "0440"
}

template {
  contents = <<EOH
username: "{{ with secret "secret/data/prometheus" }}{{ .Data.data.pg_db_metrics_user_name }}{{ end }}"
password: "{{ with secret "secret/data/prometheus" }}{{ .Data.data.pg_db_metrics_user_password }}{{ end }}"
EOH
  destination = "/run/secrets/pg_db_metrics_creds.env"
  perms       = "0440"
}
