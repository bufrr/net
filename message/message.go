package message

import "github.com/bufrr/net/util"

// GenID generates a random message id
func GenID(msgIDBytes uint8) ([]byte, error) {
	id, err := util.RandBytes(int(msgIDBytes))
	if err != nil {
		return nil, err
	}
	return id, nil
}
