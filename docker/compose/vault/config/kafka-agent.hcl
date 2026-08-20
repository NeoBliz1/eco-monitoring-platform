vault {
  address = "http://vault-server:8200"
}

auto_auth {
  method "approle" {
    config {
      role_id_file_path   = "__ROLE_ID_PATH__"
      secret_id_file_path = "__SECRET_ID_PATH__"
      remove_secret_id_file_after_reading = false
    }
  }
}

template {
  contents = <<EOH
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="{{ with secret "secret/data/kafka" }}{{ .Data.data.admin_user }}{{ end }}"
    password="{{ with secret "secret/data/kafka" }}{{ .Data.data.admin_pass }}{{ end }}";
};
EOH
  destination = "/run/secrets/kafka_server_jaas"
  perms       = "0666"
}

template {
  contents = <<EOH
KAFKA_ADMIN="{{ with secret "secret/data/kafka" }}{{ .Data.data.admin_user }}{{ end }}"
KAFKA_ADMIN_PASSWORD="{{ with secret "secret/data/kafka" }}{{ .Data.data.admin_pass }}{{ end }}"
KAFKA_REGISTRY="{{ with secret "secret/data/kafka" }}{{ .Data.data.registry_user }}{{ end }}"
KAFKA_REGISTRY_PASSWORD="{{ with secret "secret/data/kafka" }}{{ .Data.data.registry_pass }}{{ end }}"
KAFKA_VECTOR="{{ with secret "secret/data/kafka" }}{{ .Data.data.vector_user }}{{ end }}"
KAFKA_VECTOR_PASSWORD="{{ with secret "secret/data/kafka" }}{{ .Data.data.vector_pass }}{{ end }}"
KAFKA_CLIENT="{{ with secret "secret/data/kafka" }}{{ .Data.data.client_user }}{{ end }}"
KAFKA_CLIENT_PASSWORD="{{ with secret "secret/data/kafka" }}{{ .Data.data.client_pass }}{{ end }}"
EOH
  destination = "/run/secrets/kafka_scram_credentials.env"
  perms       = "0666"
}

