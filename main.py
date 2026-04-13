import os
import uuid
import ffmpeg
from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException, Depends
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "outputs")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ConversionTask(Base):
    __tablename__ = "conversions"
    id = Column(String, primary_key=True, index=True)
    original_name = Column(String)
    status = Column(String, default="pending") # pending, processing, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

app = FastAPI()
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def run_conversion(task_id: str, input_path: str, output_path: str):
    db = SessionLocal()
    task = db.query(ConversionTask).filter(ConversionTask.id == task_id).first()
    
    try:
        task.status = "processing"
        db.commit()

        (
            ffmpeg
            .input(input_path)
            .output(output_path, vn=None, acodec='libmp3lame')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        
        task.status = "completed"
    except Exception as e:
        task.status = "failed"
        print(f"Error: {e}")
    finally:
        db.commit()
        db.close()

@app.post("/convert/mp4-to-mp3")
async def start_conversion(file: UploadFile, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    task_id = str(uuid.uuid4())
    input_path = os.path.join(UPLOAD_DIR, f"{task_id}_{file.filename}")
    output_filename = f"{task_id}.mp3"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    new_task = ConversionTask(id=task_id, original_name=file.filename)
    db.add(new_task)
    db.commit()

    with open(input_path, "wb") as buffer:
        buffer.write(await file.read())

    background_tasks.add_task(run_conversion, task_id, input_path, output_path)

    return {"task_id": task_id, "status": "pending"}

@app.get("/status/{task_id}")
async def check_status(task_id: str, db: Session = Depends(get_db)):
    task = db.query(ConversionTask).filter(ConversionTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": task.id,
        "status": task.status,
        "download_url": f"/download/{task.id}" if task.status == "completed" else None
    }

@app.get("/download/{task_id}")
async def download_file(task_id: str):
    file_path = os.path.join(OUTPUT_DIR, f"{task_id}.mp3")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='audio/mpeg', filename="converted.mp3")
    raise HTTPException(status_code=404, detail="File not ready or not found")