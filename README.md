# go-pgvcr

This a testing library meant to record postgresql interactions once so the test can be replayed after without a real postgres server, just by replaying the records.

Once the records are done, this method is faster than a real Postgres and requires less resources, which is great for CI systems.

It also requires less manual work than mocks, and thus is less error-prone : SQL queries are tested at least once against a real server.

The idea is to emulate a real Postgres server at the TCP level so you can use any driver you want.

In recording mode, we just forward all messages between the client and the server and record them.

In replaying mode, we verify that the messages we receive from the client match what we recorded, and we reply with the recorded server messages.
