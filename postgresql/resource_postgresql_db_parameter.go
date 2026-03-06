package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	dbParamNameAttr     = "name"
	dbParamDatabaseAttr = "database"
)

func resourcePostgreSQLDBParameter() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLDBParameterCreate),
		Read:   PGResourceFunc(resourcePostgreSQLDBParameterRead),
		Delete: PGResourceFunc(resourcePostgreSQLDBParameterDelete),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			dbParamNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the configuration parameter (e.g. myext.setting)",
			},
			dbParamDatabaseAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The database to set the parameter for",
			},
		},
	}
}

func resourcePostgreSQLDBParameterCreate(db *DBConnection, d *schema.ResourceData) error {
	paramName := d.Get(dbParamNameAttr).(string)
	database := d.Get(dbParamDatabaseAttr).(string)

	// ALTER DATABASE does not run inside a transaction block
	conn, err := db.client.config.NewClient(database).Connect()
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("ALTER DATABASE %s SET %s TO ''", pq.QuoteIdentifier(database), paramName)
	if _, err := conn.Exec(sql); err != nil {
		return fmt.Errorf("error setting parameter %q on database %q: %w", paramName, database, err)
	}

	d.SetId(generateDBParameterID(database, paramName))
	return resourcePostgreSQLDBParameterReadImpl(db, d)
}

func resourcePostgreSQLDBParameterRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLDBParameterReadImpl(db, d)
}

func resourcePostgreSQLDBParameterReadImpl(db *DBConnection, d *schema.ResourceData) error {
	database, paramName, err := getDBParameterFromID(d, db.client.databaseName)
	if err != nil {
		return err
	}

	var exists bool
	err = db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_db_role_setting, unnest(setconfig) AS cfg
			WHERE setdatabase = (SELECT oid FROM pg_database WHERE datname = $1)
			  AND setrole = 0
			  AND cfg LIKE $2 || '=%'
		)`, database, paramName).Scan(&exists)

	switch {
	case err == sql.ErrNoRows || !exists:
		log.Printf("[WARN] PostgreSQL db parameter (%s) not found for database %s", paramName, database)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("error reading db parameter: %w", err)
	}

	d.Set(dbParamNameAttr, paramName)
	d.Set(dbParamDatabaseAttr, database)
	d.SetId(generateDBParameterID(database, paramName))
	return nil
}

func resourcePostgreSQLDBParameterDelete(db *DBConnection, d *schema.ResourceData) error {
	paramName := d.Get(dbParamNameAttr).(string)
	database := d.Get(dbParamDatabaseAttr).(string)

	conn, err := db.client.config.NewClient(database).Connect()
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("ALTER DATABASE %s RESET %s", pq.QuoteIdentifier(database), paramName)
	if _, err := conn.Exec(sql); err != nil {
		return fmt.Errorf("error resetting parameter %q on database %q: %w", paramName, database, err)
	}

	d.SetId("")
	return nil
}

func generateDBParameterID(database, paramName string) string {
	return strings.Join([]string{database, paramName}, "|")
}

func getDBParameterFromID(d *schema.ResourceData, defaultDatabase string) (string, string, error) {
	database := d.Get(dbParamDatabaseAttr).(string)
	paramName := d.Get(dbParamNameAttr).(string)

	if paramName == "" {
		parts := strings.SplitN(d.Id(), "|", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid db_parameter ID %q, expected format 'database|param_name'", d.Id())
		}
		database = parts[0]
		paramName = parts[1]
	}

	if database == "" {
		database = defaultDatabase
	}

	return database, paramName, nil
}
