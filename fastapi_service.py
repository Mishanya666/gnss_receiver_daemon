import os
import subprocess
from fastapi import FastAPI, HTTPException

# Инициализация FastAPI
app = FastAPI()

# Словарь для отслеживания запущенных процессов приемников
running_processes = {}

@app.post("/start/{receiver_name}")
async def start_receiver(receiver_name: str):
    """
    Запускает процесс приемника с указанным именем.
    Если приемник уже запущен, возвращает статус 'already running'.
    """
    try:
        # Проверка, запущен ли уже процесс с этим именем
        if receiver_name in running_processes:
            return {"status": "already running", "receiver": receiver_name}
        
        # Запуск нового процесса
        process = subprocess.Popen(["python3", "receiver_service.py", receiver_name])
        running_processes[receiver_name] = process
        return {"status": "started", "receiver": receiver_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting receiver: {e}")

@app.get("/receivers")
async def list_receivers():
    """
    Возвращает список доступных приемников, 
    найденных в папке rnx_files.
    """
    try:
        receiver_dirs = os.listdir("rnx_files")
        return {"receivers": receiver_dirs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing receivers: {e}")

@app.get("/running")
async def list_running_receivers():
    """
    Возвращает список имен запущенных приемников.
    """
    running_receiver_names = list(running_processes.keys())
    return {"running_receivers": running_receiver_names}

@app.post("/stop/{receiver_name}")
async def stop_receiver(receiver_name: str):
    """
    Останавливает процесс приемника с указанным именем.
    Если приемник не запущен, возвращает статус 'not running'.
    """
    try:
        if receiver_name not in running_processes:
            return {"status": "not running", "receiver": receiver_name}

        # Остановка процесса и удаление из списка запущенных процессов
        process = running_processes.pop(receiver_name)
        process.terminate()
        return {"status": "stopped", "receiver": receiver_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error stopping receiver: {e}")

# Запуск приложения FastAPI через Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
