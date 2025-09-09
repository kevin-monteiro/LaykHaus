#!/usr/bin/env python3
"""
Mock REST API Server for LaykHaus Demo
Provides solar energy data through REST endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import random
import json

app = FastAPI(title="Solar Energy REST API", version="1.0.0")

# Enable CORS for LaykHaus UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for recent data
telemetry_buffer = []
weather_buffer = []
consumption_buffer = []
alerts_buffer = []

# Pydantic models
class TelemetryData(BaseModel):
    timestamp: str
    panel_id: int
    power_output_watts: float
    voltage: float
    current_amps: float
    temperature_celsius: float
    efficiency_percentage: float

class WeatherData(BaseModel):
    timestamp: str
    station_id: int
    temperature_celsius: float
    humidity_percentage: float
    wind_speed_ms: float
    solar_radiation_wm2: float
    cloud_cover_percentage: int

class ConsumptionData(BaseModel):
    timestamp: str
    customer_id: int
    current_usage_kw: float
    daily_consumption_kwh: float
    peak_demand_kw: float

class Alert(BaseModel):
    timestamp: str
    alert_id: str
    panel_id: int
    alert_type: str
    severity: str
    message: str

# Root endpoint
@app.get("/")
def root():
    return {
        "name": "Solar Energy REST API",
        "status": "operational",
        "endpoints": [
            "/health",
            "/api/panels",
            "/api/panels/{panel_id}/current",
            "/api/weather/current",
            "/api/consumption/summary",
            "/api/analytics/production",
            "/api/alerts/recent"
        ]
    }

# Health check
@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Solar Panels endpoints
@app.get("/api/panels")
def get_panels():
    """Get list of all solar panels with current status."""
    panels = []
    for i in range(1, 21):  # Return 20 panels
        panels.append({
            "panel_id": i,
            "serial_number": f"SP-2024-{i:05d}",
            "location": random.choice(["Phoenix Solar Farm", "California Desert Array", "Texas Solar Ranch"]),
            "status": "active" if random.random() > 0.1 else "maintenance",
            "capacity_watts": 350 + random.randint(0, 150),
            "current_output_watts": random.uniform(200, 400) if 6 <= datetime.now().hour <= 18 else 0,
            "efficiency": random.uniform(18, 23),
            "temperature": random.uniform(25, 45),
            "last_updated": datetime.now().isoformat()
        })
    return {"panels": panels, "total": len(panels)}

@app.get("/api/panels/{panel_id}/current")
def get_panel_current(panel_id: int):
    """Get current real-time data for a specific panel."""
    if panel_id < 1 or panel_id > 100:
        raise HTTPException(status_code=404, detail="Panel not found")
    
    is_daylight = 6 <= datetime.now().hour <= 18
    
    return {
        "panel_id": panel_id,
        "timestamp": datetime.now().isoformat(),
        "metrics": {
            "power_output_watts": random.uniform(200, 400) if is_daylight else 0,
            "voltage": random.uniform(380, 420) if is_daylight else 0,
            "current_amps": random.uniform(5, 15) if is_daylight else 0,
            "temperature_celsius": random.uniform(25, 45),
            "efficiency_percentage": random.uniform(15, 22) if is_daylight else 0,
            "daily_energy_kwh": random.uniform(10, 50),
            "monthly_energy_mwh": random.uniform(0.3, 1.5),
            "co2_saved_kg": random.uniform(100, 500)
        },
        "status": {
            "operational": is_daylight,
            "grid_connected": True,
            "inverter_status": "active" if is_daylight else "standby",
            "last_maintenance": (datetime.now() - timedelta(days=random.randint(1, 90))).isoformat()
        }
    }

@app.get("/api/panels/{panel_id}/history")
def get_panel_history(panel_id: int, hours: int = 24):
    """Get historical data for a panel."""
    history = []
    now = datetime.now()
    
    for i in range(hours):
        timestamp = now - timedelta(hours=i)
        is_daylight = 6 <= timestamp.hour <= 18
        
        history.append({
            "timestamp": timestamp.isoformat(),
            "power_output_watts": random.uniform(200, 400) if is_daylight else 0,
            "efficiency_percentage": random.uniform(15, 22) if is_daylight else 0
        })
    
    return {
        "panel_id": panel_id,
        "period_hours": hours,
        "data": history
    }

# Weather endpoints
@app.get("/api/weather/current")
def get_current_weather():
    """Get current weather data from all stations."""
    stations = []
    locations = [
        ("Phoenix", 33.4484, -112.0740),
        ("Los Angeles", 34.0522, -118.2437),
        ("Austin", 30.2672, -97.7431),
        ("Las Vegas", 36.1699, -115.1398),
        ("Miami", 25.7617, -80.1918)
    ]
    
    for i, (city, lat, lon) in enumerate(locations, 1):
        stations.append({
            "station_id": i,
            "location": city,
            "latitude": lat,
            "longitude": lon,
            "timestamp": datetime.now().isoformat(),
            "temperature_celsius": random.uniform(20, 40),
            "humidity_percentage": random.uniform(20, 80),
            "wind_speed_ms": random.uniform(0, 20),
            "wind_direction_degrees": random.randint(0, 360),
            "solar_radiation_wm2": random.uniform(500, 1000) if 6 <= datetime.now().hour <= 18 else 0,
            "cloud_cover_percentage": random.randint(0, 100),
            "uv_index": random.uniform(0, 11) if 6 <= datetime.now().hour <= 18 else 0
        })
    
    return {"stations": stations, "timestamp": datetime.now().isoformat()}

@app.get("/api/weather/forecast")
def get_weather_forecast(days: int = 7):
    """Get weather forecast."""
    forecast = []
    
    for day in range(days):
        date = (datetime.now() + timedelta(days=day)).date()
        forecast.append({
            "date": str(date),
            "temperature_high": random.uniform(30, 45),
            "temperature_low": random.uniform(15, 25),
            "solar_radiation_peak": random.uniform(800, 1100),
            "cloud_cover_avg": random.randint(0, 60),
            "precipitation_probability": random.randint(0, 30),
            "optimal_production_hours": random.randint(6, 10)
        })
    
    return {"forecast": forecast, "days": days}

# Energy Consumption endpoints
@app.get("/api/consumption/summary")
def get_consumption_summary():
    """Get energy consumption summary."""
    return {
        "timestamp": datetime.now().isoformat(),
        "current": {
            "total_consumption_kw": random.uniform(1000, 5000),
            "solar_generation_kw": random.uniform(800, 4000) if 6 <= datetime.now().hour <= 18 else 0,
            "grid_import_kw": random.uniform(0, 1000),
            "grid_export_kw": random.uniform(0, 500) if 6 <= datetime.now().hour <= 18 else 0
        },
        "today": {
            "consumption_kwh": random.uniform(10000, 50000),
            "generation_kwh": random.uniform(8000, 40000),
            "self_consumption_percentage": random.uniform(60, 95),
            "cost_saved": random.uniform(100, 500)
        },
        "customers": {
            "total": 50,
            "residential": 30,
            "commercial": 15,
            "industrial": 5
        }
    }

@app.get("/api/consumption/customers/{customer_id}")
def get_customer_consumption(customer_id: int):
    """Get specific customer consumption data."""
    return {
        "customer_id": customer_id,
        "timestamp": datetime.now().isoformat(),
        "current_usage_kw": random.uniform(0.5, 10),
        "daily_consumption_kwh": random.uniform(10, 100),
        "monthly_consumption_mwh": random.uniform(0.3, 3),
        "solar_offset_percentage": random.uniform(20, 80),
        "billing": {
            "current_month_cost": random.uniform(50, 500),
            "savings_this_month": random.uniform(20, 200),
            "carbon_offset_kg": random.uniform(100, 1000)
        }
    }

# Analytics endpoints
@app.get("/api/analytics/production")
def get_production_analytics():
    """Get production analytics."""
    return {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_panels": 100,
            "active_panels": 95,
            "total_capacity_mw": 0.4,
            "current_production_mw": random.uniform(0.2, 0.35) if 6 <= datetime.now().hour <= 18 else 0
        },
        "performance": {
            "average_efficiency": random.uniform(18, 21),
            "capacity_factor": random.uniform(20, 30),
            "availability": random.uniform(95, 99.9)
        },
        "trends": {
            "daily_trend": random.choice(["increasing", "stable", "decreasing"]),
            "weekly_change_percentage": random.uniform(-5, 10),
            "monthly_change_percentage": random.uniform(-3, 8)
        }
    }

@app.get("/api/analytics/roi")
def get_roi_analytics():
    """Get return on investment analytics."""
    return {
        "timestamp": datetime.now().isoformat(),
        "financial": {
            "total_investment": 5000000,
            "revenue_to_date": random.uniform(1000000, 2000000),
            "operating_costs": random.uniform(50000, 150000),
            "net_profit": random.uniform(800000, 1800000),
            "roi_percentage": random.uniform(15, 35),
            "payback_years": random.uniform(5, 8)
        },
        "environmental": {
            "co2_saved_tons": random.uniform(1000, 5000),
            "trees_equivalent": random.randint(50000, 250000),
            "homes_powered": random.randint(500, 2000)
        }
    }

# Alerts endpoints
@app.get("/api/alerts/recent")
def get_recent_alerts(limit: int = 10):
    """Get recent system alerts."""
    alerts = []
    alert_types = [
        ("low_efficiency", "warning", "Panel efficiency below threshold"),
        ("high_temperature", "warning", "Temperature exceeds safe limit"),
        ("inverter_fault", "critical", "Inverter communication lost"),
        ("maintenance_due", "info", "Scheduled maintenance required"),
        ("grid_fault", "critical", "Grid connection issue detected"),
        ("weather_warning", "info", "Adverse weather conditions expected")
    ]
    
    for i in range(limit):
        alert_type, severity, message = random.choice(alert_types)
        alerts.append({
            "alert_id": f"ALT-{random.randint(1000, 9999)}",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "panel_id": random.randint(1, 100),
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
            "resolved": random.choice([True, False])
        })
    
    return {"alerts": alerts, "total": len(alerts)}

# Maintenance endpoints
@app.get("/api/maintenance/schedule")
def get_maintenance_schedule():
    """Get maintenance schedule."""
    schedule = []
    
    for i in range(10):
        schedule.append({
            "maintenance_id": f"MNT-{random.randint(1000, 9999)}",
            "panel_id": random.randint(1, 100),
            "scheduled_date": (datetime.now() + timedelta(days=random.randint(1, 30))).date().isoformat(),
            "type": random.choice(["cleaning", "inspection", "repair", "replacement"]),
            "priority": random.choice(["low", "medium", "high"]),
            "estimated_hours": random.uniform(0.5, 4),
            "technician": f"Tech-{random.randint(1, 10)}"
        })
    
    return {"schedule": schedule, "upcoming_count": len(schedule)}

# Data ingestion endpoints (for the generator)
@app.post("/api/telemetry")
def receive_telemetry(data: List[Dict[str, Any]]):
    """Receive telemetry data from generator."""
    global telemetry_buffer
    telemetry_buffer.extend(data)
    telemetry_buffer = telemetry_buffer[-100:]  # Keep last 100 records
    return {"status": "accepted", "count": len(data)}

@app.post("/api/weather")
def receive_weather(data: List[Dict[str, Any]]):
    """Receive weather data from generator."""
    global weather_buffer
    weather_buffer.extend(data)
    weather_buffer = weather_buffer[-100:]
    return {"status": "accepted", "count": len(data)}

@app.post("/api/consumption")
def receive_consumption(data: List[Dict[str, Any]]):
    """Receive consumption data from generator."""
    global consumption_buffer
    consumption_buffer.extend(data)
    consumption_buffer = consumption_buffer[-100:]
    return {"status": "accepted", "count": len(data)}

@app.post("/api/alerts")
def receive_alerts(data: Dict[str, Any]):
    """Receive alert data from generator."""
    global alerts_buffer
    alerts_buffer.append(data)
    alerts_buffer = alerts_buffer[-50:]
    return {"status": "accepted"}

# Get buffered data
@app.get("/api/buffer/telemetry")
def get_telemetry_buffer():
    """Get recent telemetry from buffer."""
    return {"data": telemetry_buffer[-20:], "total": len(telemetry_buffer)}

@app.get("/api/buffer/weather")
def get_weather_buffer():
    """Get recent weather from buffer."""
    return {"data": weather_buffer[-20:], "total": len(weather_buffer)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)