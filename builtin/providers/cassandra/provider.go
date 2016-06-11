package cassandra

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

var quoteReplacer = strings.NewReplacer(`"`, `\"`)

// Provider returns a terraform.ResourceProvider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		ResourcesMap: map[string]*schema.Resource{
			"cassandra_keyspace": ResourceKeyspace(),
		},

		Schema: map[string]*schema.Schema{
			"hostport": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				DefaultFunc: schema.EnvDefaultFunc(
					"CASSANDRA_HOSTPORT", "localhost:9042",
				),
			},
			"username": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("CASSANDRA_USERNAME", ""),
			},
			"password": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("CASSANDRA_PASSWORD", ""),
			},
		},

		ConfigureFunc: Configure,
	}
}

func Configure(d *schema.ResourceData) (interface{}, error) {
	hostPortRegex, err := regexp.Compile(".*:\\d*")
	if err != nil {
		return nil, fmt.Errorf("Invalid regex: %s", err)
	}

	hostPort := d.Get("hostport").(string)
	if !hostPortRegex.MatchString(hostPort) {
		return nil, fmt.Errorf("invalid Cassandra Hostport: %s", err)
	}

	cluster := gocql.NewCluster(hostPort)
	cluster.ProtoVersion = 1
	cluster.Keyspace = "system"

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return session, nil
}

func quoteIdentifier(ident string) string {
	return fmt.Sprintf(`"%s"`, quoteReplacer.Replace(ident))
}
