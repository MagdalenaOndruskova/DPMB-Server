from typing import Union, Tuple, List

from pydantic import BaseModel


class RoutingRequestBody(BaseModel):
    src_street: str | None
    dst_street: str | None
    src_coord: Union[Tuple[float, float], None]
    dst_coord: Union[Tuple[float, float], None]
    pass_streets: Union[List[str], None]
    from_time: str | None
    to_time: str | None
