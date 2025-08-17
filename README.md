# backendforautobot

- Auto-refreshes weekly NIFTY options after expiry and includes NIFTY future in live feed.

## Log configuration

Logging levels for the backend services are controlled through environment
variables. The `application.properties` file uses placeholders such as

```
logging.level.com.trader.backend.service.LiveFeedService=${LIVEFEED_LOG_LEVEL:INFO}
```

Set the appropriate environment variable to adjust the verbosity for a
specific component. The provided `railway.json` defines defaults for
production (set to `INFO`) and debug (set to `DEBUG`) deployments, but these
can be overridden to suit your needs.
