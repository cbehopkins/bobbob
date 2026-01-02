package vault

import "github.com/cbehopkins/bobbob/yggdrasil/types"

// Re-export the canonical type map implementation to avoid duplication.
type (
	ShortCodeType = types.ShortUIntKey
	TypeMap       = types.TypeMap
)

var ShortCodeLess = types.ShortCodeLess

func NewTypeMap() *TypeMap {
	return types.NewTypeMap()
}
