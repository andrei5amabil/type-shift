import os
import uuid
import ffmpeg
import magic
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
    target_format = Column(String)

Base.metadata.create_all(bind=engine)

app = FastAPI()

from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open("index.html", "r") as f:
        return f.read()

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

        if output_path.endswith(('.jpg', '.jpeg')):
            (
                ffmpeg
                .input(input_path)
                .output(output_path, **{'q:v': 2})
                .overwrite_output()
                .run()
            )
        elif output_path.endswith('.mp3'):
            (
                ffmpeg
                .input(input_path)
                .output(output_path, vn=None, acodec='libmp3lame')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )  
        elif output_path.endswith('.gif'):
            instream = ffmpeg.input(input_path)
            
            split = instream.filter_multi_output('split')
            
            palette = split[1].filter('palettegen')
            
            (
                ffmpeg
                .filter([split[0], palette], 'paletteuse')
                .output(output_path)
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
        elif output_path.endswith('.webp'):
            (
                ffmpeg
                .input(input_path)
                .output(output_path, lossless=0, quality=80) # Adjust quality as needed
                .overwrite_output()
                .run()
            )
        elif output_path.endswith('.ogg'):
            (
                ffmpeg
                .input(input_path)
                .output(output_path, acodec='libvorbis')
                .overwrite_output()
                .run()
            )
        elif output_path.endswith('.pdf') and input_path.endswith('.pdf'):
            import subprocess
            cmd = [
                "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=/ebook", "-dNOPAUSE", "-dQUIET", "-dBATCH",
                f"-sOutputFile={output_path}", input_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Ghostscript error: {result.stderr}")
        task.status = "completed"
    except Exception as e:
        task.status = "failed"
        print(f"Error: {e}")
    finally:
        db.commit()
        db.close()

@app.post("/convert/{target_format}")
async def start_conversion(
    target_format: str, 
    file: UploadFile, 
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db)
):
    header = await file.read(2048)
    file.file.seek(0)
    
    mime_type = magic.from_buffer(header, mime=True)
    if target_format == "pdf" and mime_type != "application/pdf":
        raise HTTPException(
            status_code=400, 
            detail=f"Source file is {mime_type}. PDF compression requires a PDF input."
        )

    if target_format == "mp3" and not (mime_type.startswith("video/") or mime_type.startswith("audio/")):
        raise HTTPException(status_code=400, detail=f"Cannot convert {mime_type} to MP3")
        
    if target_format in ["jpg", "jpeg", "png"] and not mime_type.startswith("image/"):
        raise HTTPException(status_code=400, detail=f"Source is not an image (detected: {mime_type})")

    supported = ["mp3", "jpg", "jpeg", "gif", "webp", "ogg", "pdf"]
    if target_format not in supported:
        raise HTTPException(status_code=400, detail="Format not supported")

    task_id = str(uuid.uuid4())
    input_path = os.path.join(UPLOAD_DIR, f"{task_id}_{file.filename}")
    
    output_filename = f"{task_id}.{target_format}"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    new_task = ConversionTask(
        id=task_id, 
        original_name=file.filename,
        target_format=target_format  
    )
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
async def download_file(task_id: str, db: Session = Depends(get_db)):
    task = db.query(ConversionTask).filter(ConversionTask.id == task_id).first()
    
    if not task or task.status != "completed":
        raise HTTPException(status_code=404, detail="File not ready or task not found")
    
    file_path = os.path.join(OUTPUT_DIR, f"{task_id}.{task.target_format}")

    if os.path.exists(file_path):
        media_types = {
            "mp3": "audio/mpeg",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "ogg": "audio/ogg"
        }
        m_type = media_types.get(task.target_format, "application/octet-stream")

        return FileResponse(
            file_path, 
            media_type=m_type, 
            filename=f"converted.{task.target_format}"
        )
        
    raise HTTPException(status_code=404, detail="Physical file missing from storage")