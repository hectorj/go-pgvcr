// Package postgrecho is a tool to mock your Postgres database in your automated tests.
//
// Same concept as https://github.com/cockroachdb/copyist, but:
// - postgrecho mocks the server, not the driver, which means you can use any driver you want (eg. pgx, though it is forced in text mode)
// - postgrecho does not have a global state, which means you can start one server for each test and run them in parallel (though if you overdo it your computer might run out of resources)
// - postgrecho supports concurrent queries to the same server (though it does cheat a bit by stalling out-of-order queries)
//
// postgrecho is not recommended for:
// - performance testing
// - driver testing
// - race condition testing (unless you enable the strict mode, which forces you to always run your query in the same order, effectively eliminating database race conditions)
package postgrecho
