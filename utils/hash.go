package utils

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
)

func Hash(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	hash := md5.Sum(data)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash[:]), nil
}
