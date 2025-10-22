from pydantic import BaseModel
from typing import Optional, Dict

class N8nTrackingInfo(BaseModel):
    BookingNo: Optional[str] = ""
    BlNumber: Optional[str] = ""
    BookingStatus: Optional[str] = ""
    Pol: Optional[str] = ""
    Pod: Optional[str] = ""
    Etd: Optional[str] = ""
    Atd: Optional[str] = ""
    Eta: Optional[str] = ""
    Ata: Optional[str] = ""
    TransitPort: Optional[str] = ""
    EtdTransit: Optional[str] = ""
    AtdTransit: Optional[str] = ""
    EtaTransit: Optional[str] = ""
    AtaTransit: Optional[str] = ""

    class Config:
        str_strip_whitespace = True


class Result(BaseModel):
    ResultData: Optional[N8nTrackingInfo] = None
    Status: Optional[int] = None
    Error: bool = False
    Errors: Optional[Dict[str, str]] = None
    Message: Optional[str] = ""
    MessageStatus: Optional[str] = ""

    class Config:
        str_strip_whitespace = True