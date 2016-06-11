package cassandra

import (
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
)

func TestSimpleReplicationDatabase(t *testing.T) {
	resource.Test(t, resource.TestCase{
		Providers: testProviders,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccDatabaseConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "name", "terraform-test",
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "durable_writes", "1",
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "replication_class", ReplicationStrategySimple,
					),
				),
			},
		},
	})
}

var testAccDatabaseConfig = `

resource "cassandra_keyspace" "test" {
    name = "terraform-test"
    durable_writes = 1
    replication_class = "` + ReplicationStrategySimple + `"
}

`
