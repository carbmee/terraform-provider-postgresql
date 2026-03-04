package postgresql

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	foreignSchemaSchemaNameAttr   = "schema_name"
	foreignSchemaRemoteSchemaAttr = "remote_schema"
	foreignSchemaServerNameAttr   = "server_name"
	foreignSchemaDatabaseAttr     = "database"
	foreignSchemaLimitToAttr      = "limit_to"
	foreignSchemaExceptAttr       = "except"
	foreignSchemaOptionsAttr      = "options"
	foreignSchemaOwnerAttr        = "owner"
	foreignSchemaDropCascadeAttr  = "drop_cascade"
)

func resourcePostgreSQLForeignSchema() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLForeignSchemaCreate),
		Read:   PGResourceFunc(resourcePostgreSQLForeignSchemaRead),
		Update: PGResourceFunc(resourcePostgreSQLForeignSchemaUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLForeignSchemaDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLForeignSchemaExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			foreignSchemaSchemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the local schema to create and import foreign tables into",
			},
			foreignSchemaRemoteSchemaAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The remote schema to import from (e.g. \"public\")",
			},
			foreignSchemaServerNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the foreign server to import the schema from",
			},
			foreignSchemaDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "The database where the schema will be created",
			},
			foreignSchemaLimitToAttr: {
				Type:          schema.TypeList,
				Optional:      true,
				ForceNew:      true,
				Elem:          &schema.Schema{Type: schema.TypeString},
				ConflictsWith: []string{foreignSchemaExceptAttr},
				Description:   "Import only the listed foreign tables. Conflicts with except",
			},
			foreignSchemaExceptAttr: {
				Type:          schema.TypeList,
				Optional:      true,
				ForceNew:      true,
				Elem:          &schema.Schema{Type: schema.TypeString},
				ConflictsWith: []string{foreignSchemaLimitToAttr},
				Description:   "Import all foreign tables except the listed ones. Conflicts with limit_to",
			},
			foreignSchemaOptionsAttr: {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Options passed to the foreign-data wrapper via WITH OPTIONS (...)",
			},
			foreignSchemaOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The role name that will own the created schema",
			},
			foreignSchemaDropCascadeAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "When true (default), drop the schema and all its objects on destroy. Set to false to require the schema to be empty before dropping",
			},
		},
	}
}

func resourcePostgreSQLForeignSchemaCreate(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureImportForeignSchema) {
		return fmt.Errorf(
			"postgresql_foreign_schema is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	database := getDatabase(d, db.client.databaseName)

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	var rolesToGrant []string

	dbOwner, err := getDatabaseOwner(txn, database)
	if err != nil {
		return err
	}
	rolesToGrant = append(rolesToGrant, dbOwner)

	if schemaOwner := d.Get(foreignSchemaOwnerAttr).(string); schemaOwner != "" && schemaOwner != dbOwner {
		rolesToGrant = append(rolesToGrant, schemaOwner)
	}

	if err := withRolesGranted(txn, rolesToGrant, func() error {
		if err := createForeignSchema(txn, d); err != nil {
			return err
		}
		return importForeignSchema(txn, d)
	}); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("error committing foreign schema: %w", err)
	}

	d.SetId(generateForeignSchemaID(d, database))

	return resourcePostgreSQLForeignSchemaReadImpl(db, d)
}

func createForeignSchema(txn *sql.Tx, d *schema.ResourceData) error {
	schemaName := d.Get(foreignSchemaSchemaNameAttr).(string)

	b := bytes.NewBufferString("CREATE SCHEMA ")
	fmt.Fprint(b, pq.QuoteIdentifier(schemaName))

	if v, ok := d.GetOk(foreignSchemaOwnerAttr); ok {
		fmt.Fprint(b, " AUTHORIZATION ", pq.QuoteIdentifier(v.(string)))
	}

	if _, err := txn.Exec(b.String()); err != nil {
		return fmt.Errorf("error creating schema %s: %w", schemaName, err)
	}
	return nil
}

func importForeignSchema(txn *sql.Tx, d *schema.ResourceData) error {
	b := bytes.NewBufferString("IMPORT FOREIGN SCHEMA ")
	fmt.Fprint(b, pq.QuoteIdentifier(d.Get(foreignSchemaRemoteSchemaAttr).(string)))

	if v, ok := d.GetOk(foreignSchemaLimitToAttr); ok {
		tables := v.([]interface{})
		quoted := make([]string, len(tables))
		for i, t := range tables {
			quoted[i] = pq.QuoteIdentifier(t.(string))
		}
		fmt.Fprintf(b, " LIMIT TO (%s)", strings.Join(quoted, ", "))
	} else if v, ok := d.GetOk(foreignSchemaExceptAttr); ok {
		tables := v.([]interface{})
		quoted := make([]string, len(tables))
		for i, t := range tables {
			quoted[i] = pq.QuoteIdentifier(t.(string))
		}
		fmt.Fprintf(b, " EXCEPT (%s)", strings.Join(quoted, ", "))
	}

	fmt.Fprintf(b, " FROM SERVER %s", pq.QuoteIdentifier(d.Get(foreignSchemaServerNameAttr).(string)))
	fmt.Fprintf(b, " INTO %s", pq.QuoteIdentifier(d.Get(foreignSchemaSchemaNameAttr).(string)))

	if options, ok := d.GetOk(foreignSchemaOptionsAttr); ok {
		optMap := options.(map[string]interface{})
		if len(optMap) > 0 {
			fmt.Fprint(b, " OPTIONS ( ")
			i := 0
			for k, v := range optMap {
				if i > 0 {
					fmt.Fprint(b, ", ")
				}
				fmt.Fprintf(b, "%s %s", pq.QuoteIdentifier(k), pq.QuoteLiteral(v.(string)))
				i++
			}
			fmt.Fprint(b, " )")
		}
	}

	if _, err := txn.Exec(b.String()); err != nil {
		return fmt.Errorf("error importing foreign schema: %w", err)
	}
	return nil
}

func resourcePostgreSQLForeignSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureImportForeignSchema) {
		return fmt.Errorf(
			"postgresql_foreign_schema is not supported for this Postgres version (%s)",
			db.version,
		)
	}
	return resourcePostgreSQLForeignSchemaReadImpl(db, d)
}

func resourcePostgreSQLForeignSchemaReadImpl(db *DBConnection, d *schema.ResourceData) error {
	database, schemaName, err := getForeignSchemaDBSchemaName(d, db.client.databaseName)
	if err != nil {
		return err
	}

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	var owner string
	err = txn.QueryRow(
		"SELECT pg_catalog.pg_get_userbyid(n.nspowner) FROM pg_catalog.pg_namespace n WHERE n.nspname = $1",
		schemaName,
	).Scan(&owner)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL foreign schema (%s) not found in database %s", schemaName, database)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("error reading foreign schema: %w", err)
	}

	d.Set(foreignSchemaSchemaNameAttr, schemaName)
	d.Set(foreignSchemaOwnerAttr, owner)
	d.Set(foreignSchemaDatabaseAttr, database)
	d.SetId(generateForeignSchemaID(d, database))

	return nil
}

func resourcePostgreSQLForeignSchemaUpdate(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureImportForeignSchema) {
		return fmt.Errorf(
			"postgresql_foreign_schema is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	if !d.HasChange(foreignSchemaOwnerAttr) {
		return resourcePostgreSQLForeignSchemaReadImpl(db, d)
	}

	database := getDatabase(d, db.client.databaseName)

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	schemaName := d.Get(foreignSchemaSchemaNameAttr).(string)
	newOwner := d.Get(foreignSchemaOwnerAttr).(string)

	sql := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s",
		pq.QuoteIdentifier(schemaName),
		pq.QuoteIdentifier(newOwner),
	)
	if _, err := txn.Exec(sql); err != nil {
		return fmt.Errorf("error updating foreign schema owner: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("error committing foreign schema update: %w", err)
	}

	return resourcePostgreSQLForeignSchemaReadImpl(db, d)
}

func resourcePostgreSQLForeignSchemaDelete(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureImportForeignSchema) {
		return fmt.Errorf(
			"postgresql_foreign_schema is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	database := getDatabase(d, db.client.databaseName)

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	schemaName := d.Get(foreignSchemaSchemaNameAttr).(string)

	exists, err := schemaExists(txn, schemaName)
	if err != nil {
		return err
	}
	if !exists {
		d.SetId("")
		return nil
	}

	owner := d.Get(foreignSchemaOwnerAttr).(string)

	if err := withRolesGranted(txn, []string{owner}, func() error {
		dropMode := "CASCADE"
		if !d.Get(foreignSchemaDropCascadeAttr).(bool) {
			dropMode = "RESTRICT"
		}
		sql := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(schemaName), dropMode)
		if _, err := txn.Exec(sql); err != nil {
			return fmt.Errorf("error dropping foreign schema: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("error committing foreign schema deletion: %w", err)
	}

	d.SetId("")
	return nil
}

func resourcePostgreSQLForeignSchemaExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	database, schemaName, err := getForeignSchemaDBSchemaName(d, db.client.databaseName)
	if err != nil {
		return false, err
	}

	exists, err := dbExists(db, database)
	if err != nil || !exists {
		return false, err
	}

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)

	var found string
	err = txn.QueryRow(
		"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = $1",
		schemaName,
	).Scan(&found)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error checking foreign schema existence: %w", err)
	}
	return true, nil
}

func generateForeignSchemaID(d *schema.ResourceData, database string) string {
	return strings.Join([]string{
		getDatabase(d, database),
		d.Get(foreignSchemaSchemaNameAttr).(string),
	}, ".")
}

// getForeignSchemaDBSchemaName parses the resource ID when importing
// (format: "database.schema_name") or reads from resource data directly.
func getForeignSchemaDBSchemaName(d *schema.ResourceData, defaultDatabase string) (string, string, error) {
	database := getDatabase(d, defaultDatabase)
	schemaName := d.Get(foreignSchemaSchemaNameAttr).(string)

	if schemaName == "" {
		parsed := strings.Split(d.Id(), ".")
		if len(parsed) != 2 {
			return "", "", fmt.Errorf(
				"foreign schema ID %q has unexpected format, expected \"database.schema_name\"",
				d.Id(),
			)
		}
		database = parsed[0]
		schemaName = parsed[1]
	}
	return database, schemaName, nil
}
