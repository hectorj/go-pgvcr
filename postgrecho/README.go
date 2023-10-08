// Package postgrecho is a tool to mock your Postgres database in your automated tests.
//
// Same concept as https://github.com/cockroachdb/copyist, but:
// - postgrecho mocks the server, not the driver, which means you can use any driver you want (eg. pgx, though it is forced in text mode)
// - postgrecho does not have a global state, which means you can start one server for each test and run them in parallel (though if you overdo it your computer might run out of resources)
// - postgrecho supports concurrent queries to the same server (though it does cheat a bit by stalling out-of-order queries)
//
// postgrecho is recommended when:
// - you want to test your SQL queries but you don't want to spin up a Postgresql Server in your CI nor when you make a non-SQL related change
//
// postgrecho is not recommended when:
// - testing performances (the proxy may add a non-constant overhead in recording mode, and the original query latency is not replayed)
// - testing database race conditions (unless you enable the strict mode, which forces you to always run your queries in the same order, effectively eliminating database race conditions by forbidding concurrency)
package postgrecho
