package utils

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"
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

func DeterministicUUID(seed any) (uuid.UUID, error) {
	byteHash, err := Hash(seed)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.FromBytes([]byte(byteHash[0:16]))
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}
