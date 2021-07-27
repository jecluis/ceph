from fastapi import APIRouter, Request
from mgr_module import MgrModule

from fastapitest.globalstate import GlobalState


router = APIRouter(prefix="/health", tags=["health"])


@router.get("/")
async def get_health(request: Request):
    gstate: GlobalState = request.app.state.gstate
    mgr: MgrModule = gstate.mgr

    return mgr.get("health")
