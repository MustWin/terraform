package cassandra

import (
	"testing"

	"fmt"
	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"log"
	"strings"
)

func TestSimpleReplicationDatabase(t *testing.T) {
	var keyspaceDesc string

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccDatabaseConfig,
				Check: resource.ComposeTestCheckFunc(
					checkKeyspaceExists("terraformTest", &keyspaceDesc),
					checkKeyspaceProperties(keyspaceDesc, "'replication_factor': '2'"),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "name", "terraformTest",
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "durable_writes", "5", // TODO: This should fail, why doesn't it?
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "replication_class", ReplicationStrategySimple,
					),
				),
			},
		},
	})
}

func TestAlterNetworkReplicationDatabase(t *testing.T) {
	var keyspaceDesc string

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccDatabaseConfig,
				Check: resource.ComposeTestCheckFunc(
					checkKeyspaceExists("terraformTest", &keyspaceDesc),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "replication_class", ReplicationStrategyNetworkTopology,
					),
				),
			},
		},
	})
}

func checkKeyspaceExists(name string, keyspaceDesc *string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		conn := testAccProvider.Meta().(*gocql.Session)
		log.Println("DESCRIBING KEYSPACE")
		err := conn.Query("DESCRIBE KEYSPACE ?", name).Scan(keyspaceDesc)
		if err != nil {
			log.Println("Returning error: %s", err)
			return err
		}
		return nil
	}
}

func checkKeyspaceProperties(keyspaceDesc string, stringsToMatch ...string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		for _, match := range stringsToMatch {
			if !strings.Contains(keyspaceDesc, match) {
				return fmt.Errorf("Keyspace description did not match expected string: `%s`", match)
			}
		}

		return nil
	}
}

const (
	testAccDatabaseConfig = `

resource "cassandra_keyspace" "test" {
    name = "terraformTest"
    durable_writes = 1
    replication_class = "` + ReplicationStrategySimple + `"
    replication_factor = 2
}

`
	testNetworkTopologyConfig = `
resource "cassandra_keyspace" "test" {
    name = "terraformTest"
    durable_writes = 1
    replication_class = "` + ReplicationStrategyNetworkTopology + `"
    datacenters = { "DC0" : 1, "DC1" : 2 }
}

`

)
