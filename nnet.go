package nnet

import (
	"github.com/bufrr/net/config"
	"github.com/bufrr/net/log"
	"github.com/bufrr/net/node"
	"github.com/bufrr/net/overlay"
	"github.com/bufrr/net/overlay/chord"
	"github.com/bufrr/net/util"
)

// NNet is is a peer to peer network
type NNet struct {
	overlay.Network
}

// Config is an alias of config.Config for simpler usage
type Config config.Config

// NewNNet creates a new nnet using the local node id and configuration
// provided. If id is nil, a random id will be generated. Empty fields in conf
// will be filled with the default config.
func NewNNet(id []byte, conf *Config) (*NNet, error) {
	var mergedConf *config.Config
	var err error

	if conf != nil {
		convertedConf := config.Config(*conf)
		mergedConf, err = config.MergedConfig(&convertedConf)
		if err != nil {
			return nil, err
		}
	} else {
		mergedConf = config.DefaultConfig()
	}

	if len(id) == 0 {
		id, err = util.RandBytes(int(mergedConf.NodeIDBytes))
		if err != nil {
			return nil, err
		}
	}

	localNode, err := node.NewLocalNode(id[:], mergedConf)
	if err != nil {
		return nil, err
	}

	network, err := chord.NewChord(localNode)
	if err != nil {
		return nil, err
	}

	nn := &NNet{
		Network: network,
	}

	return nn, nil
}

// GetConfig returns the config of nnet
func (nn *NNet) GetConfig() *Config {
	return (*Config)(nn.GetLocalNode().Config)
}

// SetLogger sets the global logger
func SetLogger(logger log.Logger) error {
	return log.SetLogger(logger)
}
