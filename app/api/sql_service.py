"""SQL 服务相关的 API 路由。"""

import asyncio
import json
import logging
from enum import Enum
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.agent.sql_agent import SQLAgent
from app.config import Config
from app.datasource import DataSourceManager
from app.exceptions import DatabaseError
from app.llm import LLM
from app.tool.data_generator import DataGeneratorTool
from app.tool.sql_review import SQLReviewTool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sql", tags=["SQL服务"])


class TaskType(str, Enum):
    """任务类型枚举。"""

    REVIEW = "review"
    DATA_GENERATION = "data_generation"


class SQLRequest(BaseModel):
    """SQL请求模型。"""

    task_type: TaskType = TaskType.REVIEW  # 默认为 SQL 审查
    sql: str
    ds_name: Optional[str] = None  # 数据源名称，可选，默认使用主数据源


class SQLResponse(BaseModel):
    """SQL响应模型。"""

    status: str
    message: str
    data: Optional[dict] = None


class DataSourceResponse(BaseModel):
    """数据源响应模型。"""

    success: bool
    message: str
    data: Optional[List[Dict]] = None


class DataSourceTestResponse(BaseModel):
    """数据源测试响应模型。"""

    success: bool
    message: str
    latency: Optional[float] = None


# 创建数据源管理器实例
config = Config()
datasource_manager = DataSourceManager(config)


async def process_sql_task(
    task_type: TaskType, sql: str, ds_name: Optional[str] = None
):
    """处理SQL任务的流式对话生成器，类似ChatGPT模式。

    Args:
        task_type: 任务类型
        sql: SQL语句
        ds_name: 数据源名称

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

        # 检查数据源连接
        if ds_name:
            yield {
                "content": f"🔗 正在连接数据源 `{ds_name}`...\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 测试数据源连接
            connection_ok = await datasource_manager.test_connection(ds_name)
            if not connection_ok:
                yield {
                    "content": f"❌ **错误：数据源 `{ds_name}` 连接失败**\n\n请检查数据源配置是否正确。\n\n",
                    "role": "assistant",
                    "type": "error",
                }
                return

            yield {
                "content": f"✅ 数据源 `{ds_name}` 连接成功！\n\n",
                "role": "assistant",
                "type": "text",
            }

        # 开始分析
        if task_type == TaskType.REVIEW:
            yield {
                "content": "🔍 **开始SQL审查分析...**\n\n我将从以下几个方面进行分析：\n- SQL语法检查\n- 性能分析\n- 安全性检查\n- 最佳实践建议\n\n",
                "role": "assistant",
                "type": "text",
            }
        else:
            yield {
                "content": "🛠️ **开始数据生成分析...**\n\n我将为你分析SQL结构并生成测试数据。\n\n",
                "role": "assistant",
                "type": "text",
            }

        yield {
            "content": "⚡ **AI专家正在深度分析...**\n\n",
            "role": "assistant",
            "type": "text",
        }

        # 初始化SQLAgent
        agent = SQLAgent()

        # 直接使用工具而不是agent.run，以获得更好的控制
        if task_type == TaskType.REVIEW:
            from app.tool.sql_review import SQLReviewTool

            tool = SQLReviewTool(config=config)

            # 准备工具参数
            tool_params = {"sql": sql}
            if ds_name:
                tool_params["ds_name"] = ds_name

            # 执行SQL审查工具
            yield {
                "content": "📊 正在执行SQL审查...\n\n",
                "role": "assistant",
                "type": "text",
            }

            tool_result = await tool.execute(**tool_params)

            if tool_result and hasattr(tool_result, "output"):
                result_content = tool_result.output

                # 解析结果并流式输出
                if "综合评分:" in result_content:
                    import re

                    score_match = re.search(r"综合评分:\s*(\d+)/100", result_content)
                    if score_match:
                        score = int(score_match.group(1))
                        emoji = (
                            "🎉"
                            if score >= 90
                            else "👍" if score >= 80 else "⚠️" if score >= 60 else "❌"
                        )
                        level = (
                            "优秀"
                            if score >= 90
                            else (
                                "良好"
                                if score >= 80
                                else "及格" if score >= 60 else "需要改进"
                            )
                        )

                        yield {
                            "content": f"### {emoji} 综合评分：{score}/100 ({level})\n\n",
                            "role": "assistant",
                            "type": "text",
                        }

                # 🔍 优先查找过滤条件建议
                if "=== 🔍 表级过滤条件建议 ===" in result_content:
                    filter_start = result_content.find("=== 🔍 表级过滤条件建议 ===")

                    # 查找过滤条件建议结束位置 - 寻找下一个 === 标记
                    filter_end = result_content.find(
                        "=== 📋 详细审查报告 ===", filter_start
                    )
                    if filter_end == -1:
                        # 如果找不到详细报告，寻找下一个 === 标记
                        next_section = result_content.find("===", filter_start + 50)
                        if next_section != -1:
                            filter_end = next_section
                        else:
                            filter_end = len(result_content)  # 使用整个内容的末尾

                    if filter_end > filter_start:
                        filter_section = result_content[filter_start:filter_end].strip()

                        yield {
                            "content": f"## 🔍 数据仓库过滤条件建议\n\n基于您数据仓库的元数据分析，我发现以下重要的过滤条件建议：\n\n",
                            "role": "assistant",
                            "type": "text",
                        }

                        # 分离建议部分和警告部分
                        if "⚠️ **检测到缺少的关键过滤条件**:" in filter_section:
                            warning_start = filter_section.find(
                                "⚠️ **检测到缺少的关键过滤条件**:"
                            )

                            # 过滤条件建议部分（不包含警告）
                            suggestion_part = filter_section[:warning_start].strip()
                            warning_part = filter_section[warning_start:].strip()

                            # 输出建议部分
                            if suggestion_part:
                                yield {
                                    "content": suggestion_part.replace(
                                        "=== 🔍 表级过滤条件建议 ===", ""
                                    ).strip()
                                    + "\n\n",
                                    "role": "assistant",
                                    "type": "filter_suggestions",
                                }

                            # 输出警告部分
                            if warning_part:
                                yield {
                                    "content": "### 🚨 重要警告：缺少关键过滤条件\n\n",
                                    "role": "assistant",
                                    "type": "warning",
                                }

                                yield {
                                    "content": warning_part + "\n\n",
                                    "role": "assistant",
                                    "type": "warning",
                                }
                        else:
                            # 只有建议，没有警告
                            yield {
                                "content": filter_section.replace(
                                    "=== 🔍 表级过滤条件建议 ===", ""
                                ).strip()
                                + "\n\n",
                                "role": "assistant",
                                "type": "filter_suggestions",
                            }

                # 查找详细报告
                if "=== 📋 详细审查报告 ===" in result_content:
                    # 提取详细报告内容
                    report_start = result_content.find("=== 📋 详细审查报告 ===")
                    report_end = result_content.find(
                        "=== ⚙️ 执行计划分析 ===", report_start
                    )
                    if report_end == -1:
                        report_end = len(result_content)

                    report_content = result_content[report_start:report_end].strip()

                    yield {
                        "content": "## 📋 AI专家审查分析\n\n",
                        "role": "assistant",
                        "type": "text",
                    }

                    # 正确按 ## 分割详细报告内容
                    sections = report_content.split("## ")

                    for section in sections[1:]:  # 跳过第一个空部分
                        if section.strip():
                            lines = section.split("\n")
                            section_title = lines[0].strip()

                            # 输出标题
                            yield {
                                "content": f"### {section_title}\n\n",
                                "role": "assistant",
                                "type": "text",
                            }

                            # 处理内容
                            current_content = ""
                            in_code_block = False

                            for line in lines[1:]:
                                if line.strip().startswith("```"):
                                    # 处理代码块
                                    if current_content.strip():
                                        yield {
                                            "content": f"{current_content.strip()}\n\n",
                                            "role": "assistant",
                                            "type": "text",
                                        }
                                        current_content = ""

                                    # 收集整个代码块 - 修复索引错误
                                    code_block = [line]
                                    current_line_idx = None

                                    # 安全地找到当前行的索引
                                    for i, l in enumerate(lines):
                                        if l == line and i >= len(
                                            [x for x in lines[:i] if x == line]
                                        ):
                                            current_line_idx = i
                                            break

                                    if current_line_idx is not None:
                                        # 从当前行的下一行开始查找结束标记
                                        for idx in range(
                                            current_line_idx + 1, len(lines)
                                        ):
                                            code_block.append(lines[idx])
                                            if lines[idx].strip().startswith("```"):
                                                break

                                    yield {
                                        "content": f"{chr(10).join(code_block)}\n\n",
                                        "role": "assistant",
                                        "type": "text",
                                    }

                                elif line.strip().startswith("### "):
                                    # 处理三级标题
                                    if current_content.strip():
                                        yield {
                                            "content": f"{current_content.strip()}\n\n",
                                            "role": "assistant",
                                            "type": "text",
                                        }
                                        current_content = ""

                                    yield {
                                        "content": f"**{line.strip()}**\n\n",
                                        "role": "assistant",
                                        "type": "text",
                                    }

                                elif line.strip().startswith(
                                    ("1. ", "2. ", "3. ", "- ", "* ")
                                ):
                                    # 处理列表项
                                    if current_content.strip():
                                        yield {
                                            "content": f"{current_content.strip()}\n\n",
                                            "role": "assistant",
                                            "type": "text",
                                        }
                                        current_content = ""

                                    yield {
                                        "content": f"{line.strip()}\n\n",
                                        "role": "assistant",
                                        "type": "text",
                                    }

                                else:
                                    # 普通内容行
                                    current_content += line + "\n"

                            # 输出最后的内容
                            if current_content.strip():
                                yield {
                                    "content": f"{current_content.strip()}\n\n",
                                    "role": "assistant",
                                    "type": "text",
                                }

                else:
                    # 如果没有标准格式，直接输出结果
                    yield {
                        "content": f"## 📋 SQL审查报告\n\n{result_content}\n\n",
                        "role": "assistant",
                        "type": "text",
                    }

        else:  # DATA_GENERATION
            from app.tool.data_generator import DataGeneratorTool

            tool = DataGeneratorTool(config=config)

            # 准备工具参数
            tool_params = {"sql": sql}
            if ds_name:
                tool_params["ds_name"] = ds_name

            # 执行数据生成工具
            yield {
                "content": "📊 正在分析表结构并生成测试数据...\n\n",
                "role": "assistant",
                "type": "text",
            }

            tool_result = await tool.execute(**tool_params)

            if tool_result and hasattr(tool_result, "output"):
                result_content = tool_result.output

                yield {
                    "content": f"## 📊 数据生成报告\n\n{result_content}\n\n",
                    "role": "assistant",
                    "type": "text",
                }

        # 输出总结
        yield {
            "content": "---\n\n## 🎯 分析总结\n\n",
            "role": "assistant",
            "type": "text",
        }

        if task_type == TaskType.REVIEW:
            yield {
                "content": "✅ SQL审查已完成！我已经从语法、性能、安全性和最佳实践等方面对你的SQL进行了全面分析。\n\n如果你有任何疑问或需要进一步的建议，请随时告诉我！\n\n",
                "role": "assistant",
                "type": "text",
            }
        else:
            yield {
                "content": "✅ 数据生成分析已完成！我已经为你分析了SQL结构并提供了相应的数据生成方案。\n\n如果需要调整或有其他需求，请随时联系我！\n\n",
                "role": "assistant",
                "type": "text",
            }

        # 结束标记
        yield {
            "content": "",
            "role": "assistant",
            "type": "done",
            "metadata": {
                "task_type": task_type.value,
                "datasource": ds_name or "default",
                "sql": sql,
                "completed": True,
            },
        }

    except Exception as e:
        yield {
            "content": f"❌ **分析过程中出现错误**\n\n错误信息：{str(e)}\n\n请检查SQL语句或数据源配置是否正确。\n\n",
            "role": "assistant",
            "type": "error",
            "metadata": {
                "error": str(e),
                "task_type": task_type.value if "task_type" in locals() else "unknown",
            },
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
            async for message in process_sql_task(
                request.task_type, request.sql, request.ds_name
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

    return StreamingResponse(
        generate_chat_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
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
        # 检查数据源连接
        if request.ds_name:
            connection_ok = await datasource_manager.test_connection(request.ds_name)
            if not connection_ok:
                raise HTTPException(
                    status_code=400, detail=f"数据源 '{request.ds_name}' 连接失败"
                )

        # 初始化 SQLAgent 实例
        agent = SQLAgent()

        # 根据任务类型构建提示词
        if request.task_type == TaskType.REVIEW:
            if request.ds_name:
                prompt = f"请帮我 REVIEW 以下在数据源 '{request.ds_name}' 中的 SQL：\n{request.sql}"
            else:
                prompt = f"请帮我 REVIEW 以下 SQL：\n{request.sql}"
        else:
            if request.ds_name:
                prompt = f"请帮我基于以下在数据源 '{request.ds_name}' 中的 SQL 进行造数：\n{request.sql}"
            else:
                prompt = f"请帮我基于以下 SQL 进行造数：\n{request.sql}"

        # 调用 SQLAgent 处理任务
        result = await agent.run(prompt)

        return SQLResponse(
            status="success",
            message="处理完成",
            data={"result": result, "datasource": request.ds_name or "default"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasources", response_model=DataSourceResponse)
async def list_datasources():
    """获取所有可用数据源列表。

    Returns:
        DataSourceResponse: 数据源列表响应
    """
    try:
        datasources = await datasource_manager.list_datasources()
        return DataSourceResponse(
            success=True, message="获取数据源列表成功", data=datasources
        )
    except Exception as e:
        logger.error(f"获取数据源列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取数据源列表失败: {str(e)}")


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
