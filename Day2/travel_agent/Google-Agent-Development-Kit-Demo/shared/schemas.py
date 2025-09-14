from pydantic import BaseModel

class TravelRequest(BaseModel):
    destination: str
    start_date: str
    end_date: str
    budget: float

