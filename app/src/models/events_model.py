from pydantic import BaseModel
from asyncio import Event


class Events(BaseModel):

    pause: Event
    resume: Event
    export: Event
    clear_memory: Event
    slowdown: Event
    speedup: Event
    kill: Event

    class Config:
        arbitrary_types_allowed = True