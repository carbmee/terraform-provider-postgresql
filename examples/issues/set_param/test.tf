# Tests that postgresql_db_parameter initializes a custom parameter at database level
# so that GRANT SET ON PARAMETER succeeds (requires PostgreSQL >= 15).
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

# Initialize the custom parameter at the database level so PostgreSQL
# recognizes it before we try to GRANT SET ON PARAMETER.
resource "postgresql_db_parameter" "myext_setting" {
  database = postgresql_database.mydb.name
  name     = "myext.setting"
}

# Now grant the role permission to SET the parameter.
resource "postgresql_grant" "myext_setting" {
  database    = postgresql_db_parameter.myext_setting.database
  object_type = "parameter"
  objects     = [postgresql_db_parameter.myext_setting.name]
  role        = postgresql_role.myrole.name
  privileges  = ["SET"]
}
