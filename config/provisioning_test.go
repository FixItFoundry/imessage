package config

import (
	"os"
	"strings"
	"testing"

	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
)

func TestProvisioningSecretLoadAndUpgrade(t *testing.T) {
	example, err := os.ReadFile("../example-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"disable", "synthetic-provisioning-secret", ""} {
		t.Run(secret, func(t *testing.T) {
			source := strings.Replace(string(example), "shared_secret: disable", "shared_secret: "+secret, 1)
			want := secret
			if secret == "" {
				source = strings.Replace(source, "provisioning:\n    shared_secret: \n", "", 1)
				want = "disable"
			}
			var base, old yaml.Node
			if err := yaml.Unmarshal(example, &base); err != nil {
				t.Fatal(err)
			}
			if err := yaml.Unmarshal([]byte(source), &old); err != nil {
				t.Fatal(err)
			}
			DoUpgrade(configupgrade.NewHelper(&base, &old))
			upgraded, err := yaml.Marshal(&base)
			if err != nil {
				t.Fatal(err)
			}
			var cfg Config
			if err = yaml.Unmarshal(upgraded, &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.Provisioning.SharedSecret != want || cfg.Bridge.LoginSharedSecret == want {
				t.Fatal("provisioning secret was lost or coupled to double-puppet authentication")
			}
		})
	}
}
