#!/usr/bin/env python3
"""
Mock REST API Server for LaykHaus Demo
Provides simplified solar panel data through REST endpoints
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
from datetime import datetime
import random

app = FastAPI(title="Solar Energy REST API", version="1.0.0")

# Enable CORS for LaykHaus UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Generate static panel data
def generate_panel_data() -> List[Dict[str, Any]]:
    """Generate realistic solar panel data"""
    locations = [
        "Phoenix Solar Farm",
        "California Desert Array", 
        "Texas Solar Ranch",
        "Nevada Solar Park",
        "Arizona Energy Center"
    ]
    
    statuses = ["active", "maintenance", "offline"]
    panels = []
    
    for i in range(1, 51):  # Generate 50 panels
        panels.append({
            "panel_id": i,
            "serial_number": f"SP-2024-{i:05d}",
            "location": random.choice(locations),
            "status": random.choices(statuses, weights=[85, 10, 5])[0],  # 85% active
            "capacity_watts": random.randint(350, 500),
            "current_output_watts": random.randint(0, 400) if random.random() > 0.1 else 0,  # 10% chance of 0 output
            "efficiency": round(random.uniform(18.0, 23.0), 2),
            "temperature": round(random.uniform(25.0, 45.0), 2),
            "last_updated": datetime.now().isoformat()
        })
    
    return panels

# Static data (regenerated periodically)
PANEL_DATA = generate_panel_data()

@app.get("/")
async def root():
    """API information"""
    return {
        "name": "Solar Energy REST API",
        "status": "operational",
        "endpoints": [
            "/health",
            "/api/panels"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/panels")
async def get_panels():
    """Get all solar panel data"""
    # Refresh data periodically (simulate real-time updates)
    if random.random() < 0.1:  # 10% chance to refresh data
        global PANEL_DATA
        PANEL_DATA = generate_panel_data()
    
    return {
        "panels": PANEL_DATA,
        "total": len(PANEL_DATA)
    }

@app.get("/api/panels/{panel_id}")
async def get_panel(panel_id: int):
    """Get specific panel data"""
    panel = next((p for p in PANEL_DATA if p["panel_id"] == panel_id), None)
    if not panel:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Panel not found")
    
    return panel

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)