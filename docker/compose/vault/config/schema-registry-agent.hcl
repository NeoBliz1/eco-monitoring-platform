vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "/tmp/vault_input/schema-registry_role_id"
      secret_id_file_path = "/tmp/vault_input/schema-registry_secret_id"
      remove_secret_id_file_after_reading = true
    }
  }
}

template {
  contents = <<EOH
KafkaClient {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="{{ with secret "secret/data/kafka" }}{{ .Data.data.registry_user }}{{ end }}"
    password="{{ with secret "secret/data/kafka" }}{{ .Data.data.registry_pass }}{{ end }}";
};
EOH
  destination = "/run/secrets/schema_registry_jaas"
  perms       = "0666"
}