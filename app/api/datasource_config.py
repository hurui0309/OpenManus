"""数据源配置管理API。"""

import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import Response

from app.config import Config
from app.exceptions import DatabaseError
from app.schemas.datasource import (
    APIResponse,
    DataSourceConfigCreate,
    DataSourceConfigDetail,
    DataSourceConfigList,
    DataSourceConfigResponse,
    DataSourceConfigUpdate,
    DataSourceTestResult,
)
from app.service.datasource_config_service import DataSourceConfigService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasources", tags=["数据源配置管理"])


def json_serializer(obj):
    """自定义JSON序列化器，处理datetime对象。"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def serialize_response(response_obj) -> str:
    """序列化响应对象为JSON字符串。"""
    return json.dumps(
        response_obj.model_dump(), ensure_ascii=False, default=json_serializer
    )


def get_cors_headers():
    """获取完整的 CORS 头部配置"""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept, Origin, X-Requested-With, X-User-ID",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "3600",
    }


@router.options("/")
@router.options("/{ds_name}")
@router.options("/{ds_name}/test")
async def preflight_handler():
    """处理 CORS 预检请求"""
    return Response(status_code=200, headers=get_cors_headers())


# 创建服务实例
config = Config()
datasource_service = DataSourceConfigService(config)


def get_current_user(x_user_id: Optional[str] = Header(None)) -> str:
    """获取当前用户ID。

    Args:
        x_user_id: 从请求头获取用户ID

    Returns:
        str: 用户ID

    Raises:
        HTTPException: 当用户ID未提供时
    """
    if not x_user_id:
        raise HTTPException(
            status_code=401, detail="需要提供用户ID，请在请求头中设置 X-User-ID"
        )
    return x_user_id


@router.post("/", response_model=DataSourceConfigDetail)
async def create_datasource_config(
    config_data: DataSourceConfigCreate,
    current_user: str = Header(..., alias="X-User-ID"),
):
    """创建数据源配置。

    Args:
        config_data: 数据源配置数据
        current_user: 当前用户ID

    Returns:
        DataSourceConfigDetail: 创建的数据源配置详情

    Raises:
        HTTPException: 当创建失败时
    """
    try:
        # 设置创建人
        config_data.created_by = current_user

        result = await datasource_service.create_datasource_config(config_data)

        response = DataSourceConfigDetail(
            success=True,
            message=f"数据源配置 '{config_data.ds_name}' 创建成功",
            data=result,
        )

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except DatabaseError as e:
        logger.error(f"创建数据源配置失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建数据源配置时发生错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")


@router.get("/", response_model=DataSourceConfigList)
async def list_datasource_configs(
    created_by: Optional[str] = Query(None, description="创建人过滤"),
    ds_type: Optional[str] = Query(None, description="数据源类型过滤"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小"),
):
    """获取数据源配置列表。

    Args:
        created_by: 创建人过滤条件
        ds_type: 数据源类型过滤条件
        page: 页码
        page_size: 每页大小

    Returns:
        DataSourceConfigList: 数据源配置列表

    Raises:
        HTTPException: 当查询失败时
    """
    try:
        result = await datasource_service.list_datasource_configs(
            created_by=created_by, ds_type=ds_type, page=page, page_size=page_size
        )

        response = DataSourceConfigList(
            success=True,
            message="获取数据源配置列表成功",
            data=result["configs"],
            total=result["total"],
        )

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except DatabaseError as e:
        logger.error(f"获取数据源配置列表失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取数据源配置列表时发生错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")


@router.get("/{ds_name}", response_model=DataSourceConfigDetail)
async def get_datasource_config(ds_name: str):
    """获取指定数据源配置详情。

    Args:
        ds_name: 数据源名称

    Returns:
        DataSourceConfigDetail: 数据源配置详情

    Raises:
        HTTPException: 当数据源不存在时
    """
    try:
        result = await datasource_service.get_datasource_config(ds_name)

        if not result:
            raise HTTPException(status_code=404, detail=f"数据源 '{ds_name}' 不存在")

        response = DataSourceConfigDetail(
            success=True, message=f"获取数据源配置 '{ds_name}' 成功", data=result
        )

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except HTTPException:
        raise
    except DatabaseError as e:
        logger.error(f"获取数据源配置失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取数据源配置时发生错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")


@router.put("/{ds_name}", response_model=DataSourceConfigDetail)
async def update_datasource_config(
    ds_name: str,
    config_data: DataSourceConfigUpdate,
    current_user: str = Header(..., alias="X-User-ID"),
):
    """更新数据源配置。

    Args:
        ds_name: 数据源名称
        config_data: 更新的配置数据
        current_user: 当前用户ID

    Returns:
        DataSourceConfigDetail: 更新后的数据源配置详情

    Raises:
        HTTPException: 当更新失败时
    """
    try:
        # 设置更新人
        config_data.updated_by = current_user

        result = await datasource_service.update_datasource_config(
            ds_name, config_data, current_user
        )

        response = DataSourceConfigDetail(
            success=True, message=f"数据源配置 '{ds_name}' 更新成功", data=result
        )

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except DatabaseError as e:
        logger.error(f"更新数据源配置失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新数据源配置时发生错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")


@router.delete("/{ds_name}", response_model=APIResponse)
async def delete_datasource_config(
    ds_name: str, current_user: str = Header(..., alias="X-User-ID")
):
    """删除数据源配置。

    注意：只有创建人才能删除自己创建的数据源配置。

    Args:
        ds_name: 数据源名称
        current_user: 当前用户ID

    Returns:
        APIResponse: 删除结果

    Raises:
        HTTPException: 当删除失败或无权限时
    """
    try:
        success = await datasource_service.delete_datasource_config(
            ds_name, current_user
        )

        if success:
            response = APIResponse(
                success=True, message=f"数据源配置 '{ds_name}' 删除成功"
            )
        else:
            response = APIResponse(
                success=False, message=f"数据源配置 '{ds_name}' 删除失败"
            )

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except DatabaseError as e:
        logger.error(f"删除数据源配置失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除数据源配置时发生错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")


@router.post("/{ds_name}/test", response_model=DataSourceTestResult)
async def test_datasource_connection(ds_name: str):
    """测试数据源连接。

    Args:
        ds_name: 数据源名称

    Returns:
        DataSourceTestResult: 连接测试结果

    Raises:
        HTTPException: 当测试失败时
    """
    try:
        result = await datasource_service.test_datasource_connection(ds_name)

        response = DataSourceTestResult(**result)

        return Response(
            content=serialize_response(response),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except Exception as e:
        logger.error(f"测试数据源连接时发生错误: {str(e)}", exc_info=True)
        error_result = DataSourceTestResult(
            success=False, message=f"测试数据源连接失败: {str(e)}", error_detail=str(e)
        )
        return Response(
            content=serialize_response(error_result),
            media_type="application/json",
            headers=get_cors_headers(),
        )


# 兼容性接口：保持与原有API的兼容性
@router.get("/names")
async def get_datasource_names():
    """获取数据源名称枚举列表，用于前端下拉框（兼容性接口）"""
    try:
        result = await datasource_service.list_datasource_configs(page_size=1000)

        # 转换为前端需要的选项格式
        options = []
        for config in result["configs"]:
            option = {
                "value": config.ds_name,
                "label": f"{config.ds_name} ({config.ds_type})",
                "type": config.ds_type,
                "description": f"{config.ds_type} 数据源",
                "created_by": config.created_by,
            }
            options.append(option)

        response = {
            "success": True,
            "message": "成功获取数据源名称列表",
            "data": {"total": len(options), "options": options},
        }

        return Response(
            content=serialize_response(APIResponse(**response)),
            media_type="application/json",
            headers=get_cors_headers(),
        )

    except Exception as e:
        logger.error(f"获取数据源名称列表失败: {str(e)}")
        error_response = APIResponse(
            success=False, message=f"获取数据源名称列表失败: {str(e)}", error=str(e)
        )
        return Response(
            content=serialize_response(error_response),
            media_type="application/json",
            headers=get_cors_headers(),
        )
