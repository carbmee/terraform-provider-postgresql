package postgresql

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	dbParamNameAttr     = "name"
	dbParamDatabaseAttr = "database"
)

// paramNameRe matches valid custom GUC parameter names (extension.setting).
var paramNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

func resourcePostgreSQLDBParameter() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLDBParameterCreate),
		Read:   PGResourceFunc(resourcePostgreSQLDBParameterRead),
		Delete: PGResourceFunc(resourcePostgreSQLDBParameterDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLDBParameterExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			dbParamNameAttr: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringMatch(
					paramNameRe,
					"parameter name must be in the form 'extension.setting' (e.g. myext.setting)",
				),
				Description: "The name of the custom GUC parameter (e.g. myext.setting). " +
					"Used to initialize the parameter at database level so that " +
					"GRANT SET ON PARAMETER (PostgreSQL >= 15) can reference it.",
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

	query := fmt.Sprintf("ALTER DATABASE %s SET %s TO ''", pq.QuoteIdentifier(database), paramName)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error setting parameter %q on database %q: %w", paramName, database, err)
	}

	d.SetId(generateDBParameterID(database, paramName))
	return resourcePostgreSQLDBParameterReadImpl(db, d)
}

func resourcePostgreSQLDBParameterExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	database, paramName, err := getDBParameterFromID(d, db.client.databaseName)
	if err != nil {
		return false, err
	}
	return dbParameterExists(db, database, paramName)
}

func resourcePostgreSQLDBParameterRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLDBParameterReadImpl(db, d)
}

func resourcePostgreSQLDBParameterReadImpl(db *DBConnection, d *schema.ResourceData) error {
	database, paramName, err := getDBParameterFromID(d, db.client.databaseName)
	if err != nil {
		return err
	}

	exists, err := dbParameterExists(db, database, paramName)
	if err != nil {
		return fmt.Errorf("error reading db parameter: %w", err)
	}
	if !exists {
		log.Printf("[WARN] PostgreSQL db parameter (%s) not found for database %s", paramName, database)
		d.SetId("")
		return nil
	}

	d.Set(dbParamNameAttr, paramName)
	d.Set(dbParamDatabaseAttr, database)
	d.SetId(generateDBParameterID(database, paramName))
	return nil
}

func resourcePostgreSQLDBParameterDelete(db *DBConnection, d *schema.ResourceData) error {
	paramName := d.Get(dbParamNameAttr).(string)
	database := d.Get(dbParamDatabaseAttr).(string)

	query := fmt.Sprintf("ALTER DATABASE %s RESET %s", pq.QuoteIdentifier(database), paramName)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error resetting parameter %q on database %q: %w", paramName, database, err)
	}

	d.SetId("")
	return nil
}

func dbParameterExists(db *DBConnection, database, paramName string) (bool, error) {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM pg_db_role_setting, unnest(setconfig) AS cfg
			WHERE setdatabase = (SELECT oid FROM pg_database WHERE datname = $1)
			  AND setrole = 0
			  AND cfg LIKE $2 || '=%'
		)`, database, paramName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
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
