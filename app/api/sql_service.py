"""SQL 服务相关的 API 路由。"""

import asyncio
import json
import logging
from enum import Enum
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator

from app.config import Config
from app.datasource import DataSourceManager
from app.exceptions import DatabaseError
from app.llm import LLM
from app.schemas.datasource import DataSourceConfigResponse
from app.tool.data_generator import DataGeneratorTool
from app.tool.sql_review import SQLReviewTool
from app.tool.text_parser import get_text_parser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sql", tags=["SQL服务"])


def get_cors_headers():
    """获取完整的 CORS 头部配置"""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept, Origin, X-Requested-With",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "3600",
    }


@router.options("/process")
@router.options("/datasources")
@router.options("/datasources/names")
@router.options("/datasources/{ds_name}/test")
async def preflight_handler(request: Request):
    """处理 CORS 预检请求"""
    from fastapi.responses import Response

    return Response(status_code=200, headers=get_cors_headers())


class TaskType(str, Enum):
    """任务类型枚举。"""

    REVIEW = "review"
    DATA_GENERATION = "data_generation"


class SQLRequest(BaseModel):
    """SQL请求模型。"""

    task_type: TaskType = TaskType.REVIEW  # 默认为 SQL 审查
    sql: Optional[str] = None  # SQL语句（review任务必填）
    ds_name: Optional[str] = None  # 数据源名称，可选，默认使用主数据源
    user_id: Optional[str] = None  # 用户ID，用于日志记录和权限控制
    # 造数任务使用mixed_input字段，包含SQL和用户要求的混合文本，由后端自动解析
    mixed_input: Optional[str] = None  # 包含SQL和用户要求的混合文本（造数任务专用）

    @field_validator("sql", "mixed_input")
    @classmethod
    def validate_task_fields(cls, v, info):
        """验证任务类型与字段的匹配性。"""
        if info.data.get("task_type") == TaskType.REVIEW:
            if info.field_name == "sql" and not v:
                raise ValueError("SQL审查任务需要提供sql字段")
        elif info.data.get("task_type") == TaskType.DATA_GENERATION:
            if info.field_name == "mixed_input" and not v:
                raise ValueError("数据生成任务需要提供mixed_input字段")
        return v


class DataSourceResponse(BaseModel):
    """数据源响应模型。"""

    success: bool
    message: str
    data: Optional[List[DataSourceConfigResponse]] = None


class DataSourceTestResponse(BaseModel):
    """数据源测试响应模型。"""

    success: bool
    message: str
    latency: Optional[float] = None


# 创建数据源管理器实例
config = Config()
datasource_manager = DataSourceManager(config)


async def process_sql_task(
    task_type: TaskType,
    sql: str,
    ds_name: Optional[str] = None,
    requirements: Optional[str] = None,
    user_id: Optional[str] = None,
):
    """处理SQL任务的流式对话生成器，类似ChatGPT模式。

    Args:
        task_type: 任务类型
        sql: SQL语句
        ds_name: 数据源名称
        requirements: 用户特殊要求（从mixed_input解析得到，仅在数据生成时使用）
        user_id: 用户ID，用于日志记录

    Yields:
        dict: 包含流式对话内容的字典
    """
    try:
        # 开始对话
        yield {
            "content": f"我来帮你分析这个SQL语句：\n\n```sql\n{sql}\n```\n\n",
            "role": "assistant",
            "type": "text",
        }

        # 如果是数据生成且有用户要求，显示要求信息
        if task_type == TaskType.DATA_GENERATION and requirements:
            yield {
                "content": f"📋 **用户要求**: {requirements}\n\n",
                "role": "assistant",
                "type": "text",
            }

        # 检查数据源连接
        if ds_name:
            yield {
                "content": f"🔗 正在连接数据源 `{ds_name}`...\n\n",
                "role": "assistant",
                "type": "text",
            }

            try:
                # 测试数据源连接
                await datasource_manager.test_connection(ds_name)
                yield {
                    "content": f"✅ 数据源 `{ds_name}` 连接成功\n\n",
                    "role": "assistant",
                    "type": "text",
                }
            except Exception as e:
                yield {
                    "content": f"❌ 数据源 `{ds_name}` 连接失败: {str(e)}\n\n",
                    "role": "assistant",
                    "type": "error",
                }
                return

        # 根据任务类型选择处理逻辑
        if task_type == TaskType.REVIEW:
            yield {
                "content": "🔍 **开始SQL审查分析...**\n\n我将从性能、安全性和最佳实践等角度为你分析这个SQL语句。\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 使用 SQL Review 工具的流式执行
            from app.tool.sql_review import SQLReviewTool

            tool = SQLReviewTool(config=config)

            # 准备工具参数
            tool_params = {"sql": sql}
            if ds_name:
                tool_params["ds_name"] = ds_name

            # 流式执行SQL审查工具
            async for message in tool.execute_stream(**tool_params):
                yield message

        else:  # DATA_GENERATION
            yield {
                "content": "🛠️ **开始数据生成分析...**\n\n我将为你分析SQL结构并生成测试数据。\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 使用 Data Generator 工具的流式执行
            from app.tool.data_generator import DataGeneratorTool

            tool = DataGeneratorTool(config=config)

            # 准备工具参数
            tool_params = {"sql": sql}
            if ds_name:
                tool_params["ds_name"] = ds_name
            if requirements:
                tool_params["requirements"] = requirements
            if user_id:
                tool_params["user_id"] = user_id

            # 流式执行数据生成工具
            async for message in tool.execute_stream(**tool_params):
                yield message

    except Exception as e:
        logger.error(f"处理SQL任务时发生错误: {str(e)}", exc_info=True)
        yield {
            "content": f"❌ **系统错误**\n\n{str(e)}\n\n",
            "role": "assistant",
            "type": "error",
        }


@router.post("/process")
async def process_sql(request: SQLRequest):
    """处理 SQL 请求的流式对话接口，类似ChatGPT。

    Args:
        request: SQL请求对象

    Returns:
        StreamingResponse: 流式对话响应
    """

    async def generate_chat_stream():
        """生成对话流式数据"""
        try:
            # 处理不同任务类型的输入解析
            sql = request.sql
            requirements = ""

            # 造数任务：必须使用mixed_input进行解析
            if request.task_type == TaskType.DATA_GENERATION:
                if not request.mixed_input:
                    error_chunk = {
                        "id": f"chatcmpl-error-{hash('missing_mixed_input')}",
                        "object": "chat.completion.chunk",
                        "created": int(asyncio.get_event_loop().time()),
                        "model": "sql-expert",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "content": "❌ **错误**: 数据生成任务需要提供 mixed_input 字段\n\n",
                                    "role": "assistant",
                                },
                                "finish_reason": "stop",
                            }
                        ],
                        "error": True,
                        "usage": None,
                    }
                    yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
                    yield "data: [DONE]\n\n"
                    return

                try:
                    text_parser = get_text_parser()
                    parsed_result = await text_parser.parse_mixed_input(
                        request.mixed_input
                    )

                    # 使用解析结果
                    sql = parsed_result.sql
                    requirements = parsed_result.requirements

                    # 显示解析结果
                    parse_chunk = {
                        "id": f"chatcmpl-parse-{hash(str(parsed_result))}",
                        "object": "chat.completion.chunk",
                        "created": int(asyncio.get_event_loop().time()),
                        "model": "sql-expert",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "content": f"📝 **文本解析结果** (置信度: {parsed_result.confidence:.2f})\n\n",
                                    "role": "assistant",
                                },
                                "finish_reason": None,
                            }
                        ],
                        "usage": None,
                    }
                    yield f"data: {json.dumps(parse_chunk, ensure_ascii=False)}\n\n"

                    if parsed_result.requirements:
                        req_chunk = {
                            "id": f"chatcmpl-req-{hash(parsed_result.requirements)}",
                            "object": "chat.completion.chunk",
                            "created": int(asyncio.get_event_loop().time()),
                            "model": "sql-expert",
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {
                                        "content": f"🎯 **提取的用户要求**: {parsed_result.requirements}\n\n",
                                        "role": "assistant",
                                    },
                                    "finish_reason": None,
                                }
                            ],
                            "usage": None,
                        }
                        yield f"data: {json.dumps(req_chunk, ensure_ascii=False)}\n\n"

                    sql_chunk = {
                        "id": f"chatcmpl-sql-{hash(sql)}",
                        "object": "chat.completion.chunk",
                        "created": int(asyncio.get_event_loop().time()),
                        "model": "sql-expert",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "content": f"💻 **提取的SQL语句**:\n```sql\n{sql}\n```\n\n",
                                    "role": "assistant",
                                },
                                "finish_reason": None,
                            }
                        ],
                        "usage": None,
                    }
                    yield f"data: {json.dumps(sql_chunk, ensure_ascii=False)}\n\n"

                except Exception as e:
                    logger.error(f"文本解析失败: {str(e)}")
                    error_chunk = {
                        "id": f"chatcmpl-parse-error-{hash(str(e))}",
                        "object": "chat.completion.chunk",
                        "created": int(asyncio.get_event_loop().time()),
                        "model": "sql-expert",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "content": f"⚠️ **文本解析失败，使用原始输入**: {str(e)}\n\n",
                                    "role": "assistant",
                                },
                                "finish_reason": None,
                            }
                        ],
                        "usage": None,
                    }
                    yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
                    sql = request.mixed_input  # 降级处理

            # SQL Review任务：使用sql字段
            else:
                sql = request.sql
                requirements = ""

            async for message in process_sql_task(
                request.task_type, sql, request.ds_name, requirements, request.user_id
            ):
                # 使用类似OpenAI ChatGPT的流式格式
                chunk = {
                    "id": f"chatcmpl-{hash(str(message))}",
                    "object": "chat.completion.chunk",
                    "created": int(asyncio.get_event_loop().time()),
                    "model": "sql-expert",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "content": message.get("content", ""),
                                "role": message.get("role", "assistant"),
                            },
                            "finish_reason": (
                                "stop" if message.get("type") == "done" else None
                            ),
                        }
                    ],
                    "usage": None,
                }

                # 如果有错误或元数据，添加到响应中
                if "metadata" in message:
                    chunk["metadata"] = message["metadata"]
                if message.get("type") == "error":
                    chunk["error"] = True

                yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                # 如果是结束消息，发送完成标记
                if message.get("type") == "done":
                    yield "data: [DONE]\n\n"
                    break

        except Exception as e:
            # 发送错误消息
            error_chunk = {
                "id": f"chatcmpl-error-{hash(str(e))}",
                "object": "chat.completion.chunk",
                "created": int(asyncio.get_event_loop().time()),
                "model": "sql-expert",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "content": f"❌ **系统错误**\n\n{str(e)}\n\n",
                            "role": "assistant",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "error": True,
                "usage": None,
            }
            yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"

    # 合并 CORS 头部和流式响应头部
    stream_headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
        **get_cors_headers(),  # 使用统一的 CORS 头部配置
    }

    return StreamingResponse(
        generate_chat_stream(),
        media_type="text/event-stream",
        headers=stream_headers,
    )


@router.get("/datasources", response_model=DataSourceResponse)
async def list_datasources():
    """获取所有可用数据源列表，包含完整配置信息。

    Returns:
        DataSourceResponse: 数据源列表响应
    """
    try:
        datasources_data = await datasource_manager.list_datasources()

        # 将字典数据转换为 DataSourceConfigResponse 模型
        datasources = []
        for ds_data in datasources_data:
            datasource = DataSourceConfigResponse(
                ds_name=ds_data["ds_name"],
                ds_type=ds_data["ds_type"],
                url=ds_data["url"],
                user=ds_data["user"],
                pwd=ds_data["pwd"],
                properties=ds_data["properties"],
                created_by=ds_data["created_by"],
                create_time=ds_data["create_time"],
                updated_by=ds_data["updated_by"],
                update_time=ds_data["update_time"],
            )
            datasources.append(datasource)

        return DataSourceResponse(
            success=True, message="获取数据源列表成功", data=datasources
        )
    except Exception as e:
        logger.error(f"获取数据源列表失败: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"获取数据源列表失败: {str(e)}",
            headers=get_cors_headers(),
        )


@router.get("/datasources/names")
async def get_datasource_names():
    """获取数据源名称枚举列表，用于前端下拉框"""
    try:
        datasources = await datasource_manager.list_datasources()

        # 转换为前端需要的选项格式
        options = []
        for ds in datasources:
            option = {
                "value": ds["ds_name"],
                "label": f"{ds['ds_name']} ({ds['ds_type']})",
                "type": ds["ds_type"],
                "description": f"{ds['ds_type']} 数据源",
            }
            options.append(option)

        return {
            "success": True,
            "message": "成功获取数据源名称列表",
            "data": {"total": len(options), "options": options},
        }
    except Exception as e:
        logger.error(f"获取数据源名称列表失败: {str(e)}")
        return {
            "success": False,
            "message": f"获取数据源名称列表失败: {str(e)}",
            "error": str(e),
        }


@router.get("/datasources/{ds_name}/test", response_model=DataSourceTestResponse)
async def test_datasource(ds_name: str):
    """测试指定数据源的连接。

    Args:
        ds_name: 数据源名称

    Returns:
        DataSourceTestResponse: 连接测试结果
    """
    try:
        import time

        start_time = time.time()

        connection_ok = await datasource_manager.test_connection(ds_name)
        latency = (time.time() - start_time) * 1000  # 转换为毫秒

        if connection_ok:
            return DataSourceTestResponse(
                success=True,
                message=f"数据源 '{ds_name}' 连接成功",
                latency=round(latency, 2),
            )
        else:
            return DataSourceTestResponse(
                success=False,
                message=f"数据源 '{ds_name}' 连接失败",
                latency=round(latency, 2),
            )
    except DatabaseError as e:
        return DataSourceTestResponse(
            success=False,
            message=f"数据源 '{ds_name}' 不存在或配置错误: {str(e)}",
            latency=None,
        )
    except Exception as e:
        logger.error(f"测试数据源连接失败: {str(e)}")
        return DataSourceTestResponse(
            success=False, message=f"测试连接时发生错误: {str(e)}", latency=None
        )
