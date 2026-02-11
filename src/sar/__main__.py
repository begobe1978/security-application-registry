"""SAR entrypoint.

Run with:
  python -m sar
"""

import os
import uvicorn

def main() -> None:
    host = os.getenv("SAR_HOST", "0.0.0.0")
    port = int(os.getenv("SAR_PORT", "8000"))
    reload = os.getenv("SAR_RELOAD", "false").lower() in {"1", "true", "yes", "y"}
    uvicorn.run("sar.app:app", host=host, port=port, reload=reload)

if __name__ == "__main__":
    main()
