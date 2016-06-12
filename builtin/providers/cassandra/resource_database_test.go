package cassandra

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestSimpleReplicationDatabase(t *testing.T) {
	var keyspaceMeta gocql.KeyspaceMetadata

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckKeyspaceDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccDatabaseConfig,
				Check: resource.ComposeTestCheckFunc(
					checkKeyspaceExists("terraformTest", &keyspaceMeta),
					checkKeyspaceProperties(&keyspaceMeta, gocql.KeyspaceMetadata{
						Name:            "terraformTest",
						DurableWrites:   true,
						StrategyClass:   ReplicationStrategySimple,
						StrategyOptions: map[string]interface{}{"replication_factor": "2"},
					}),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "name", "terraformTest",
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "durable_writes", "true", // TODO: This should fail, why doesn't it?
					),
					resource.TestCheckResourceAttr(
						"cassandra_keyspace.test", "replication_class", ReplicationStrategySimple,
					),
				),
			},
		},
	})
}
/*
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
*/
func keyspaceExists(name string) (*gocql.KeyspaceMetadata, error) {
	conn := testAccProvider.Meta().(*gocql.Session)
	return conn.KeyspaceMetadata(name)
}

func checkKeyspaceExists(name string, keyspaceMeta *gocql.KeyspaceMetadata) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		data, err := keyspaceExists(name)
		if err != nil {
			log.Println("Returning error: %s", err)
			return err
		}
		if data == nil {
			return fmt.Errorf("Keyspace not found: %s", data.Name)
		}
		*keyspaceMeta = *data
		return nil
	}
}

func checkKeyspaceProperties(actualMeta *gocql.KeyspaceMetadata, expectedMeta gocql.KeyspaceMetadata) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		fmt.Println("ExpectedName: ", expectedMeta.Name)
		fmt.Println("ActualName: ", actualMeta.Name)
		if expectedMeta.Name != "" && actualMeta.Name != expectedMeta.Name {
			return fmt.Errorf("Keyspace name %s does not match expected %s", actualMeta.Name, expectedMeta.Name)
		}
		if expectedMeta.DurableWrites != actualMeta.DurableWrites {
			return fmt.Errorf("Durable writes %s does not match expected %s", actualMeta.DurableWrites, expectedMeta.DurableWrites)
		}
		// We use Contains, because the actual class looks more like this: 'org.apache.cassandra.locator.SimpleStrategy'
		if expectedMeta.StrategyClass != "" && !strings.Contains(actualMeta.StrategyClass, expectedMeta.StrategyClass) {
			return fmt.Errorf("StrategyClass %s does not match expected %s", actualMeta.StrategyClass, expectedMeta.StrategyClass)
		}
		for key, _ := range expectedMeta.StrategyOptions {
			if key == "class" { // Already checked
				continue
			}
			if expectedMeta.StrategyOptions[key] != actualMeta.StrategyOptions[key] {
				return fmt.Errorf("Strategy options %v did not match expected string: `%v`",
					actualMeta.StrategyOptions[key],
					expectedMeta.StrategyOptions[key],
				)
			}
		}

		return nil
	}
}

func testAccCheckKeyspaceDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "cassandra_keyspace" {
			continue
		}

		data, err := keyspaceExists(rs.Primary.ID)

		if err == nil && data.Name != "" {
			return fmt.Errorf("Keyspace %s still exists", rs.Primary.ID)
		}

		if err != nil {
			fmt.Println("---------------")
			fmt.Println(err)
		}

		if !strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("Unexpected error: %s", err)
		}
	}

	return nil
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
