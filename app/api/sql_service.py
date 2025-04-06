"""SQL 服务相关的 API 路由。"""
from enum import Enum
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import asyncio
import json

from app.agent.manus import Manus
from app.config import Config
from app.exceptions import DatabaseError

router = APIRouter(prefix="/sql", tags=["SQL服务"])

class TaskType(str, Enum):
    """任务类型枚举。"""
    REVIEW = "review"
    DATA_GENERATION = "data_generation"

class SQLRequest(BaseModel):
    """SQL请求模型。"""
    task_type: TaskType
    sql: str

class SQLResponse(BaseModel):
    """SQL响应模型。"""
    status: str
    message: str
    data: Optional[dict] = None

async def process_sql_task(task_type: TaskType, sql: str):
    """处理SQL任务的异步生成器。

    Args:
        task_type: 任务类型
        sql: SQL语句

    Yields:
        dict: 包含处理状态和消息的字典
    """
    try:
        # 初始化 Manus 实例
        agent = Manus()

        # 根据任务类型构建提示词
        if task_type == TaskType.REVIEW:
            prompt = f"请帮我 REVIEW 以下 SQL：\n{sql}"
        else:
            prompt = f"请帮我基于以下 SQL 进行造数：\n{sql}"

        # 设置处理开始状态
        yield {
            "status": "processing",
            "message": f"开始处理{task_type.value}任务...",
            "data": None
        }

        # 调用 Manus 处理任务
        result = await agent.run(prompt)

        # 返回处理结果
        yield {
            "status": "success",
            "message": "处理完成",
            "data": {"result": result}
        }

    except Exception as e:
        yield {
            "status": "error",
            "message": f"处理失败：{str(e)}",
            "data": None
        }

@router.post("/process")
async def process_sql(request: SQLRequest):
    """处理 SQL 请求的流式响应接口。

    Args:
        request: SQL请求对象

    Returns:
        StreamingResponse: 流式响应对象
    """
    async def generate_events():
        async for result in process_sql_task(request.task_type, request.sql):
            # 将结果转换为 SSE 格式
            yield f"data: {json.dumps(result, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@router.post("/process_sync")
async def process_sql_sync(request: SQLRequest):
    """处理 SQL 请求的同步接口。

    Args:
        request: SQL请求对象

    Returns:
        SQLResponse: SQL响应对象
    """
    try:
        # 初始化 Manus 实例
        agent = Manus()

        # 根据任务类型构建提示词
        if request.task_type == TaskType.REVIEW:
            prompt = f"请帮我 REVIEW 以下 SQL：\n{request.sql}"
        else:
            prompt = f"请帮我基于以下 SQL 进行造数：\n{request.sql}"

        # 调用 Manus 处理任务
        result = await agent.run(prompt)

        return SQLResponse(
            status="success",
            message="处理完成",
            data={"result": result}
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
