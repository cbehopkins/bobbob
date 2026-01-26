package external_test

import (
	"github.com/cbehopkins/bobbob/allocator/basic"
	atypes "github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

// This file demonstrates external configuration of allocator callbacks
// using the Parent() method and SetOnAllocate, proving that these
// features are accessible outside the allocator package.

// ExampleConfigureAllocatorCallbacks shows how external code can
// configure allocation monitoring callbacks on both an allocator
// and its parent BasicAllocator using the exposed Parent() method.
//
// This works with any allocator created via NewOmniBlockAllocator,
// demonstrating that the callback configuration is accessible from
// outside the allocator package.
func ExampleConfigureAllocatorCallbacks(v *vault.Vault) {
	// The allocator might support callback configuration via a SetOnAllocate method
	// Type assert to the interface that provides callback configuration
	type callbackSetter interface {
		SetOnAllocate(func(atypes.ObjectId, atypes.FileOffset, int))
	}
	alloc := v.Allocator()
	if alloc == nil {
		return
	}

	if setter, ok := alloc.(callbackSetter); ok {
		setter.SetOnAllocate(func(objId atypes.ObjectId, offset atypes.FileOffset, size int) {
			// External logging/monitoring logic here
			_ = objId
			_ = offset
			_ = size
		})
	}

	// The allocator might also provide access to its parent via a Parent() method
	type parentProvider interface {
		Parent() atypes.Allocator
	}

	if provider, ok := alloc.(parentProvider); ok {
		if parent := provider.Parent(); parent != nil {
			// Type assert to BasicAllocator to access its SetOnAllocate
			if basicAlloc, ok := parent.(*basic.BasicAllocator); ok {
				basicAlloc.SetOnAllocate(func(objId atypes.ObjectId, offset atypes.FileOffset, size int) {
					// External logging/monitoring logic for parent allocations
					_ = objId
					_ = offset
					_ = size
				})
			}
		}
	}
}

// Compile-time verification that BasicAllocator.SetOnAllocate is accessible
// from outside the package. This proves the method is public.
func _verifyBasicAllocatorSetOnAllocate(a *basic.BasicAllocator) {
	a.SetOnAllocate(func(atypes.ObjectId, atypes.FileOffset, int) {})
}
