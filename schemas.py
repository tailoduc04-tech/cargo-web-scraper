from pydantic import BaseModel
from typing import Optional, Dict

class N8nTrackingInfo(BaseModel):
    BookingNo: Optional[str] = None
    BlNumber: Optional[str] = None
    BookingStatus: Optional[str] = None
    Pol: Optional[str] = None
    Pod: Optional[str] = None
    Etd: Optional[str] = None
    Atd: Optional[str] = None
    Eta: Optional[str] = None
    Ata: Optional[str] = None
    TransitPort: Optional[str] = None
    EtdTransit: Optional[str] = None
    AtdTransit: Optional[str] = None
    EtaTransit: Optional[str] = None
    AtaTransit: Optional[str] = None

    class Config:
        anystr_strip_whitespace = True


class Result(BaseModel):
    ResultData: Optional[N8nTrackingInfor] = None
    Status: Optional[int] = None
    Error: bool = False
    Errors: Optional[Dict[str, str]] = None
    Message: Optional[str] = None
    MessageStatus: Optional[str] = None

    class Config:
        anystr_strip_whitespace = True