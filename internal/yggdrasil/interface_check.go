package yggdrasil

// This file contains compile-time interface compliance checks.
// If any type doesn't properly implement its intended interface,
// compilation will fail with a clear error message.
//
// These are zero-cost abstractions - they don't generate any runtime code,
// but provide valuable compile-time safety.

// Verify TreapNode implements TreapNodeInterface
var _ TreapNodeInterface[IntKey] = (*TreapNode[IntKey])(nil)

// Verify PayloadTreapNode implements TreapNodeInterface
var _ TreapNodeInterface[IntKey] = (*PayloadTreapNode[IntKey, int])(nil)

// Verify PersistentTreapNode implements TreapNodeInterface
var _ TreapNodeInterface[IntKey] = (*PersistentTreapNode[IntKey])(nil)

// Verify PersistentTreapNode implements PersistentTreapNodeInterface
var _ PersistentTreapNodeInterface[IntKey] = (*PersistentTreapNode[IntKey])(nil)

// Note: Cannot verify PersistentPayloadTreapNode here because it requires
// a PersistentPayload type parameter which is defined in test files.
// The PersistentPayloadTreapNode type is verified in the test files instead.

// Verify IntKey implements Key
var _ Key[IntKey] = (*IntKey)(nil)

// Verify IntKey implements PersistentKey
var _ PersistentKey[IntKey] = (*IntKey)(nil)

// Verify StringKey implements Key
var _ Key[StringKey] = (*StringKey)(nil)

// Verify StringKey implements PersistentKey
var _ PersistentKey[StringKey] = (*StringKey)(nil)

// Verify ShortUIntKey implements Key
var _ Key[ShortUIntKey] = (*ShortUIntKey)(nil)

// Verify ShortUIntKey implements PersistentKey
var _ PersistentKey[ShortUIntKey] = (*ShortUIntKey)(nil)
