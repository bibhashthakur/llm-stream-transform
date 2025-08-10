import threading, uvicorn
from .kafka_loop import run_loop
from .api import app

if __name__ == "__main__":
    t = threading.Thread(target=run_loop, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8080)
