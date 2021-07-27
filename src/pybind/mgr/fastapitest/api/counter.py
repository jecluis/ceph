from fastapi import APIRouter, Request

from fastapitest.globalstate import GlobalState

router = APIRouter(prefix="/counter", tags=["counter"])


@router.get("/", response_model=int)
async def get_counter(request: Request) -> int:

    gstate: GlobalState = request.app.state.gstate
    return gstate.counter
