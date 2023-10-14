// Package grecho is a tool to mock your Postgres database in your automated tests.
//
// Same concept as https://github.com/cockroachdb/copyist, but:
// - grecho mocks the server, not the driver, which means you can use any driver you want (eg. pgx)
// - grecho does not have a global state, which means you can start one server for each test and run them in parallel (though if you overdo it your computer might run out of resources)
// - grecho supports concurrent queries to the same server (though it does cheat a bit by stalling out-of-order queries)
//
// grecho is recommended when:
// - you want to test your SQL queries but you don't want to spin up a Postgresql Server in your CI nor when you make a non-SQL related change
//
// grecho is not recommended when:
// - testing performances (the proxy adds a non-constant overhead in recording mode, and the original query latency is not replicated in replaying mode)
// - testing database race conditions (unless you enable the strict mode, which forces you to always run your queries in the same order, effectively eliminating database race conditions by forbidding concurrency)
package grecho
