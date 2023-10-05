// Package postgrecho is a tool to mock your Postgres database in your automated tests.
//
// Same concept as https://github.com/cockroachdb/copyist, but:
// - postgrecho mocks the server, not the driver, which means you can use any driver you want
// - postgrecho does not have a global state, which means you can start one server for each test and run them in parallel
// - postgrecho records session IDs, which means you can have concurrent connections to the DB
package postgrecho
