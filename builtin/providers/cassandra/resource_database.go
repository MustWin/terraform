package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
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
	conn := meta.(*gocql.Session)

	name := d.Get("name").(string)
	queryParams := make([]interface{}, 0)
	queryParams = append(queryParams, name)
	replicationStr := "{ 'class': ?"
	replicationClass := d.Get("replication_class").(string)
	if replicationClass != ReplicationStrategySimple && replicationClass != ReplicationStrategyNetworkTopology {
		return fmt.Errorf("replication_class must be one of [%s, %s]", ReplicationStrategySimple, ReplicationStrategyNetworkTopology)
	}

	switch replicationClass {
	case ReplicationStrategySimple:
		queryParams = append(queryParams, replicationClass)
		replicationFactor := d.Get("replication_factor").(string)
		replicationStr += ", replication_factor: ? }"
		queryParams = append(queryParams, replicationFactor)
	case ReplicationStrategyNetworkTopology:
		queryParams = append(queryParams, replicationClass)
		datacenters := d.Get("datacenters").(map[string]interface{})
		for datacenter, count := range datacenters {
			replicationStr += ", ?: ?"
			queryParams = append(queryParams, datacenter)
			queryParams = append(queryParams, count)
		}
		replicationStr += " }"
	}
	queryParams = append(queryParams, d.Get("durable_writes").(bool))

	err := conn.Query("CREATE KEYSPACE IF NOT EXIST ? WITH REPLICATION = " + replicationStr + " AND DURABLE_WRITES = ?").Exec()
	if err != nil {
		return err
	}

	d.SetId(name)

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
	// TODO

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
