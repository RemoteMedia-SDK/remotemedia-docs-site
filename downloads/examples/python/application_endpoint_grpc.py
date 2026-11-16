"""Run the application-endpoint example over a local gRPC control connection."""

import asyncio
from application_endpoint import main


if __name__ == "__main__":
    asyncio.run(main("grpc"))
