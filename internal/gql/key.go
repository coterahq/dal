package gql

import (
	"fmt"
)

type ResolverKey struct {
	Key any
}

func NewResolverKey(key any) *ResolverKey {
	return &ResolverKey{Key: key}
}

func (rk *ResolverKey) String() string {
	return fmt.Sprintf("%v", rk.Key)
}

func (rk *ResolverKey) Raw() any {
	return rk.Key
}
