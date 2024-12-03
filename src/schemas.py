from pydantic import BaseModel, Field, model_validator
from typing import Optional
from datetime import datetime 
import math

class OrderSchema(BaseModel):
    orderId: str
    productId: str
    currency: str
    quantity: int = Field(..., ge=0)
    shippingCost: float = Field(..., ge=0)  # Ensure positive price
    amount: float = Field(..., ge=0)  # Ensure positive price
    channel: str
    channelGroup: str
    campaign: Optional[str] = None  # Allows None or a string
    dateTime: datetime

    @model_validator(mode='before') # instead of looping all the objects
    def handle_nan_campaign(cls, values):
        # Handle NaN values in the 'campaign' field
        if isinstance(values.get('campaign'), float) and math.isnan(values.get('campaign')):
            values['campaign'] = ""  # Or None if preferred
        return values


class InventorySchema(BaseModel):
    productId: str
    name: str
    quantity: int = Field(..., ge=0)  # Ensure non-negative stock
    category: str
    subCategory: str
