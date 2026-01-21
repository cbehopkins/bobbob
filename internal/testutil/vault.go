package testutil

// TestUserData represents a user for testing purposes.
type TestUserData struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Age      int    `json:"age"`
}

// TestProductData represents a product for testing purposes.
type TestProductData struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}
