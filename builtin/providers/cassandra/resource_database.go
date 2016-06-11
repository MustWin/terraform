package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
	"strings"
)

const (
	ReplicationStrategySimple          = "SimpleStrategy"
	ReplicationStrategyNetworkTopology = "NetworkTopologyStrategy"
)

func ResourceKeyspace() *schema.Resource {
	return &schema.Resource{
		Create: CreateKeyspace,
		Read:   ReadKeyspace,
		Delete: DeleteKeyspace,
		Update: UpdateKeyspace,

		// https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html
		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: false,
			},
			"durable_writes": &schema.Schema{
				Type:     schema.TypeBool,
				Required: true,
				ForceNew: false,
			},
			"replication_class": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: false,
			},
			// Required if replication_class == "SimpleStrategy"
			"replication_factor": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: false, // TODO: add an alter command
			},
			// Required if replication_class == "NetworkTopologyStrategy"
			"datacenters": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
				ForceNew: false,
			},
		},
	}
}

func CreateKeyspace(d *schema.ResourceData, meta interface{}) error {
	replicationClass := d.Get("replication_class").(string)
	if replicationClass != ReplicationStrategySimple && replicationClass != ReplicationStrategyNetworkTopology {
		return fmt.Errorf("replication_class must be one of [%s, %s]", ReplicationStrategySimple, ReplicationStrategyNetworkTopology)
	}

	conn := meta.(*gocql.Session)
	queryStr, queryParams := CreateKeyspaceQuery(d)

	err := conn.Query(queryStr, queryParams).Exec()

	if err != nil {
		return err
	}

	return nil
}

func ReadKeyspace(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*gocql.Session)
	name := d.Id()

	iter := conn.Query("SELECT keyspace_name FROM system.schema_keyspaces", name).Iter()
	var keyspace string
	found := false
	for iter.Scan(&keyspace) {
		if keyspace == name {
			found = true
		}
	}
	err := iter.Close()
	if err != nil {
		return err
	}

	if !found {
		d.SetId("")
	}

	return nil
}

func UpdateKeyspace(d *schema.ResourceData, meta interface{}) error {
	replicationClass := d.Get("replication_class").(string)
	if replicationClass != ReplicationStrategySimple && replicationClass != ReplicationStrategyNetworkTopology {
		return fmt.Errorf("replication_class must be one of [%s, %s]", ReplicationStrategySimple, ReplicationStrategyNetworkTopology)
	}

	conn := meta.(*gocql.Session)
	queryStr, queryParams := CreateKeyspaceQuery(d)
	err := conn.Query(queryStr, queryParams).Exec()

	if err != nil {
		return err
	}

	return nil
}

func DeleteKeyspace(d *schema.ResourceData, meta interface{}) error {
	err := ReadKeyspace(d, meta)
	if err != nil {
		return err
	}

	conn := meta.(*gocql.Session)
	name := d.Id()

	if d.Id() != "" {
		err := conn.Query("DROP KEYSPACE ?", name).Exec()
		if err != nil {
			return err
		}
		d.SetId("")

	}

	return nil
}

func CreateKeyspaceQuery(d *schema.ResourceData) (string, []interface{}) {
	replicationCql, replicationParams := KeyspaceQueryFactory(d)
	query := "CREATE KEYSPACE IF NOT EXIST ? WITH REPLICATION = " + replicationCql + " AND DURABLE_WRITES = ?"
	return query, replicationParams

}

func AlterKeyspaceQuery(d *schema.ResourceData) (string, []interface{}) {
	replicationCql, replicationParams := KeyspaceQueryFactory(d)
	query := "ALTER KEYSPACE ? WITH REPLICATION = " + replicationCql + " AND DURABLE_WRITES = ?"
	return query, replicationParams
}

func KeyspaceQueryFactory(d *schema.ResourceData) (string, []interface{}) {
	name := d.Id()
	replicationStr := []string{}
	queryParams := make([]interface{}, 0)

	queryParams = append(queryParams, name)
	replicationStr = append(replicationStr, "{ 'class': ?")

	replicationClass := d.Get("replication_class").(string)
	queryParams = append(queryParams, replicationClass)

	switch replicationClass {
	case ReplicationStrategySimple:
		replicationFactor := d.Get("replication_factor").(string)
		replicationStr = append(replicationStr, ", replication_factor: ? }")
		queryParams = append(queryParams, replicationFactor)
	case ReplicationStrategyNetworkTopology:
		datacenters := d.Get("datacenters").(map[string]interface{})
		for datacenter, count := range datacenters {
			replicationStr = append(replicationStr, ", ?: ?")
			queryParams = append(queryParams, datacenter)
			queryParams = append(queryParams, count)
		}
		replicationStr = append(replicationStr, " }")
	}
	queryParams = append(queryParams, d.Get("durable_writes").(bool))

	return strings.Join(replicationStr, ""), queryParams
}
