# Tests that GRANT SET ON PARAMETER works for custom GUC parameters (requires PostgreSQL >= 15).
# The grant resource initializes the parameter in the current session automatically.
#
# Usage:
#   docker compose up -d
#   go build -o ./postgresql/terraform-provider-postgresql ../../..
#   TF_CLI_CONFIG_FILE=dev.tfrc terraform init
#   TF_CLI_CONFIG_FILE=dev.tfrc terraform apply

terraform {
  required_version = ">= 1.0"

  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = ">=1.14"
    }
  }
}

provider "postgresql" {
  host     = "localhost"
  port     = 5432
  username = "postgres"
  password = "postgres"
  database = "postgres"
  sslmode  = "disable"
}

resource "postgresql_database" "mydb" {
  name = "mydb"
}

resource "postgresql_role" "myrole" {
  name     = "myrole"
  login    = true
  password = "myrole-password"
}

# Grant the role permission to SET the custom parameter.
# initialize_params = true is required for custom GUC names not yet known to the server.
resource "postgresql_grant" "myext_setting" {
  database          = postgresql_database.mydb.name
  object_type       = "parameter"
  objects           = ["myext.setting"]
  role              = postgresql_role.myrole.name
  privileges        = ["SET"]
  initialize_parameters = true
}
