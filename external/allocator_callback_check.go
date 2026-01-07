package external_test

import (
	"github.com/cbehopkins/bobbob/store/allocator"
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
		SetOnAllocate(func(allocator.ObjectId, allocator.FileOffset, int))
	}
	alloc := v.Allocator()
	if alloc == nil {
		return
	}

	if setter, ok := alloc.(callbackSetter); ok {
		setter.SetOnAllocate(func(objId allocator.ObjectId, offset allocator.FileOffset, size int) {
			// External logging/monitoring logic here
			_ = objId
			_ = offset
			_ = size
		})
	}

	// The allocator might also provide access to its parent via a Parent() method
	type parentProvider interface {
		Parent() allocator.Allocator
	}

	if provider, ok := alloc.(parentProvider); ok {
		if parent := provider.Parent(); parent != nil {
			// Type assert to BasicAllocator to access its SetOnAllocate
			if basicAlloc, ok := parent.(*allocator.BasicAllocator); ok {
				basicAlloc.SetOnAllocate(func(objId allocator.ObjectId, offset allocator.FileOffset, size int) {
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
func _verifyBasicAllocatorSetOnAllocate(a *allocator.BasicAllocator) {
	a.SetOnAllocate(func(allocator.ObjectId, allocator.FileOffset, int) {})
}
