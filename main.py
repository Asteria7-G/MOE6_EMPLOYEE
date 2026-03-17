import uvicorn
from typing import List
from fastapi import FastAPI, BackgroundTasks, APIRouter, Request,HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from inline_schedule_tool import *
from offline_schedule_tool import *

app = FastAPI(title='AUTO_SHIFT_ARRANGEMENT')
app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
)

##############################################################
 #                 INLINE TASK               #
##############################################################
task_router = APIRouter()

# 每一条 data 里的结构
class InlineTaskItem(BaseModel):
    Year: str
    Month: str
    Date: str
    DorN: str
    Dept: str
    SubDept: str
    LineGroup: str
    LineID: str
    LineName: str
    ShiftCode: str


# 整个请求体结构
class GetInlineTask(BaseModel):
    mode: str
    type: str
    data: List[InlineTaskItem]

@task_router.post(
    '/api/auto_shift/task/',
    tags=['Task']
)
async def post_inline_task(item: GetInlineTask, request: Request):
    """
    demo:{"mode":"Regular>Outsourcing>OT",
 "type":"InLineTask",
 "data":
     [
         {
         "Year":"2026",
             "Month":"3",
             "Date":"11",
             "DorN":"D",
             "Dept":"ME/MOE6-CN"
             ,"SubDept":"ME/MFO6.5-CN",
             "LineGroup":"FA",
             "LineID":"31",
             "LineName":"BSFA12",
             "ShiftCode":"S080"
         },
         {
        "Year":"2026",
             "Month":"3",
             "Date":"11",
             "DorN":"N",
             "Dept":"ME/MOE6-CN",
             "SubDept":"ME/MFO6.5-CN",
             "LineGroup":"FA",
             "LineID":"31",
             "LineName":"BSFA12",
             "ShiftCode":"S102"
         }
     ]
 }

    return
    """
    try:
        task_type = item.type
        print(f"This round task type is {task_type}")
        follow_rule = item.mode
        print(f"This round follow rule is {follow_rule}")
        task_data = item.data
        print(f"this round task data is {task_data}")
        #先判定任务类型是 inline / offline
        if task_type == "InLineTask":
            # 再判断follow rule
            if follow_rule == 'Regular>Outsourcing>OT':
                result = inline_rule_one(task_data)
            elif follow_rule == 'Regular>OT>Outsourcing':
                result = inline_rule_two(task_data)
            elif follow_rule == 'Outsourcing>Regular>OT':
                result = inline_rule_three(task_data)

        elif task_type == "OffLineTask":
            if follow_rule == 'Regular>Outsourcing>OT':
                result = offline_rule_one(task_data)
            elif follow_rule == 'Regular>OT>Outsourcing':
                result = offline_rule_two(task_data)
            elif follow_rule == 'Outsourcing>Regular>OT':
                result = offline_rule_three(task_data)

        return JSONResponse(content={'status': 1, 'result': result}, status_code=200)
    except Exception as e:
        print(str(e))
        return JSONResponse(content={'status': 2, 'result': ''}, status_code=200)



app.include_router(task_router)
uvicorn.run(app=app, host='0.0.0.0', port=8000)