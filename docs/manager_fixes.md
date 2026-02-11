1. High: managed containers leak if Server.Start fails after backend startup
     pkg/dynolocalfaster/server.go:76 starts the backend manager, but early-return paths at pkg/
     dynolocalfaster/server.go:82 and pkg/dynolocalfaster/server.go:101 don’t call manager.Close().
     Impact: orphaned DynamoDB Local containers + subsequent Start can fail with “already started”
     inside ManagedManager.
     Recommended fix: after s.manager.Start(ctx) succeeds, use a cleanup guard (defer) that closes
     manager on any later startup error until startup is fully committed.
2. Medium: readiness check is only TCP-level, not DynamoDB API-level
     internal/backends/managed.go:195 waits for exposed port, and internal/backends/managed.go:141
     probes only host:port reachability.
     Impact: startup may report ready before DynamoDB Local can reliably serve API operations, causing
     flaky first requests.
     Recommended fix: add an API probe (for example ListTables) with retry/backoff before returning
     backends.
3. Medium (testing gap): no startup-failure cleanup test at server layer
     There are good manager unit tests, but no test covering server-level failure cleanup (for example
     API listen bind failure after manager start).
     Impact: the leak in finding #1 wasn’t caught by tests.
     Recommended test: inject a fake manager into Server, force listen failure, assert Close was
     invoked.
