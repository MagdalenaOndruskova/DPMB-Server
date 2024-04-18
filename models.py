from typing import Union, Tuple, List

from pydantic import BaseModel, EmailStr


class RoutingRequestBody(BaseModel):
    src_street: str | None
    dst_street: str | None
    src_coord: Union[Tuple[float, float], None]
    dst_coord: Union[Tuple[float, float], None]
    pass_streets: Union[List[str], None]
    from_time: str | None
    to_time: str | None


class RoutingCoordRequestBody(BaseModel):
    src_coord: Union[Tuple[float, float], None]
    dst_coord: Union[Tuple[float, float], None]
    from_time: str | None
    to_time: str | None


class PlotDataRequestBody(BaseModel):
    from_date: str | None
    to_date: str | None
    streets: Union[List[str], None]
    route: Union[List[List[float]], None]


class EmailSchema(BaseModel):
    subject: str
    body: str
    from_email: str