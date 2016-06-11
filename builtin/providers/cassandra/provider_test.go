package cassandra

import (
	"testing"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// To run these acceptance tests, you will need a Cassandra server.
// If you download a Cassandra distribution and run it with its default
// settings, on the same host where the tests are being run, then these tests
// should work with no further configuration.
//
// To run the tests against a remote Cassandra server, set the CASSANDRA_HOSTPORT,
// CASSANDRA_USERNAME and CASSANDRA_PASSWORD environment variables.

var testProviders map[string]terraform.ResourceProvider
var testProvider *schema.Provider

func init() {
	testProvider = Provider().(*schema.Provider)
	testProviders = map[string]terraform.ResourceProvider{
		"cassandra": testProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().(*schema.Provider).InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ terraform.ResourceProvider = Provider()
}
