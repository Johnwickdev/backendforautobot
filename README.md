# backendforautobot

- Auto-refreshes weekly NIFTY options after expiry and includes NIFTY future in live feed.

## Local setup

1. Copy `.env.example` to `.env` and provide values for:
   `MONGODB_URI`, `UPSTOX_API_KEY`, `UPSTOX_API_SECRET`,
   `UPSTOX_ACCESS_TOKEN` and `INFLUX_TOKEN`.
2. Load the variables into your shell, for example:
   ```bash
   export $(cat .env | xargs)
   ```
3. Run the application:
   ```bash
   ./mvnw spring-boot:run
   ```

The application reads the above credentials from environment variables. On
Railway, configure them in the project environment settings.
