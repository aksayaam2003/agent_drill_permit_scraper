from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, ValidationError
import uvicorn
import os
import uuid
from typing import List, Dict, Any
import logging
import yaml

from scraper import scrape_permits, download_plat_files
import httpx

app = FastAPI(
    title="Texas Permit Scraper Agent",
    description="An agent to scrape drilling permits from the Texas RRC website.",
    version="1.0.0"
)

# In-memory storage for job status. In a production app, use Redis or a database.
jobs: Dict[str, Dict[str, Any]] = {}

class ScrapeConfig(BaseModel):
    counties: List[str]
    date_range: Dict[str, str]

def load_config():
    """Loads the scraping configuration from config/config.yaml."""
    config_path = os.path.join('config', 'config.yaml')
    if not os.path.exists(config_path):
        raise HTTPException(status_code=500, detail="Configuration file not found.")
    with open(config_path, 'r') as f:
        try:
            config_data = yaml.safe_load(f)
            # Validate the loaded config against the Pydantic model
            return ScrapeConfig(**config_data)
        except yaml.YAMLError as e:
            raise HTTPException(status_code=500, detail=f"Error parsing YAML config: {e}")
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {e}")


async def scrape_and_save_task(job_id: str, config: ScrapeConfig):
    """The actual scraping and file processing task that runs in the background."""
    jobs[job_id]['status'] = 'running'
    try:
        df = await scrape_permits(config.model_dump())
        if not df.empty:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                df = await download_plat_files(df, client)

            csv_path = os.path.join('data', f"{job_id}_permits.csv")
            df.to_csv(csv_path, index=False)
            jobs[job_id]['status'] = 'completed'
            jobs[job_id]['result_file'] = csv_path
            logging.info(f"Job {job_id} completed successfully.")
        else:
            jobs[job_id]['status'] = 'completed_with_no_data'
            logging.info(f"Job {job_id} completed with no data.")

    except Exception as e:
        logging.error(f"Job {job_id} failed: {e}")
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = str(e)


@app.post("/scrape/", status_code=202)
async def start_scraping(background_tasks: BackgroundTasks):
    """
    Starts a new scraping job in the background using the configuration from config/config.yaml.
    """
    config = load_config()
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "config": config.model_dump()}
    background_tasks.add_task(scrape_and_save_task, job_id, config)
    return {"message": "Scraping job started using config.yaml.", "job_id": job_id}


@app.get("/scrape/status/{job_id}")
async def get_job_status(job_id: str):
    """
    Gets the status of a scraping job.
    """
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/data/files/")
async def list_data_files():
    """
    Lists all available data files (CSV and plat files).
    """
    if not os.path.exists('data'):
        return {"message": "Data directory not found."}

    all_files = []
    for root, _, files in os.walk('data'):
        for name in files:
            # Create a relative path from the 'data' directory
            relative_path = os.path.join(root, name).replace('data/', '', 1)
            all_files.append(relative_path)

    return {"files": all_files}


@app.get("/data/download/{file_path:path}")
async def download_data_file(file_path: str):
    """
    Downloads a specific data file.
    """
    full_path = os.path.join('data', file_path)
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(full_path)

# Add a root endpoint for basic API health check
@app.get("/")
def read_root():
    return {"status": "Texas Permit Scraper Agent is running"}

if __name__ == "__main__":
    # This allows running the app directly for development/testing
    uvicorn.run(app, host="0.0.0.0", port=8000)
