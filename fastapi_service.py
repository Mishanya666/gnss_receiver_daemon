import os
import subprocess
import logging
from fastapi import FastAPI, HTTPException
from typing import List

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "fastapi_service.log")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file)])

logger = logging.getLogger("FastAPIService")

app = FastAPI()
running_processes = {}

@app.post("/start/{receiver_name}")
async def start_receiver(receiver_name: str):
    try:
        if receiver_name in running_processes:
            logger.info(f"Receiver {receiver_name} is already running.")
            return {"status": "already running", "receiver": receiver_name}
        
        process = subprocess.Popen(["python3", "receiver_service.py", receiver_name])
        running_processes[receiver_name] = process
        logger.info(f"Started receiver {receiver_name}.")
        return {"status": "started", "receiver": receiver_name}
    except Exception as e:
        logger.error(f"Error starting receiver {receiver_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/receivers")
async def list_receivers():
    try:
        receiver_dirs = os.listdir("rnx_files")
        logger.info("Listed receivers.")
        return {"receivers": receiver_dirs}
    except Exception as e:
        logger.error(f"Error listing receivers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/running")
async def list_running_receivers():
    running_receiver_names = list(running_processes.keys())
    logger.info("Listed running receivers.")
    return {"running_receivers": running_receiver_names}

@app.post("/stop/{receiver_name}")
async def stop_receiver(receiver_name: str):
    try:
        if receiver_name not in running_processes:
            logger.info(f"Receiver {receiver_name} is not running.")
            return {"status": "not running", "receiver": receiver_name}

        process = running_processes.pop(receiver_name)
        process.terminate()
        logger.info(f"Stopped receiver {receiver_name}.")
        return {"status": "stopped", "receiver": receiver_name}
    except Exception as e:
        logger.error(f"Error stopping receiver {receiver_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
