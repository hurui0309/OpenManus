"""数据源配置相关的数据模型。"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class DataSourceConfigBase(BaseModel):
    """数据源配置基础模型。"""

    ds_name: str = Field(..., description="数据源名称，唯一标识符", max_length=50)
    ds_type: str = Field(..., description="数据源类型", max_length=20)
    url: str = Field(..., description="JDBC连接地址", max_length=500)
    user: str = Field(..., description="数据库用户名", max_length=50)
    pwd: str = Field(..., description="数据库密码", max_length=100)
    properties: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="额外配置参数"
    )

    @validator("ds_type")
    def validate_ds_type(cls, v):
        """验证数据源类型。"""
        allowed_types = ["mysql", "hive", "postgresql", "oracle", "sqlite"]
        if v.lower() not in allowed_types:
            raise ValueError(f'数据源类型必须是以下之一: {", ".join(allowed_types)}')
        return v.lower()

    @validator("ds_name")
    def validate_ds_name(cls, v):
        """验证数据源名称。"""
        import re

        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", v):
            raise ValueError("数据源名称必须以字母开头，只能包含字母、数字和下划线")
        return v


class DataSourceConfigCreate(DataSourceConfigBase):
    """创建数据源配置的请求模型。"""

    created_by: str = Field(..., description="创建人", max_length=50)


class DataSourceConfigUpdate(BaseModel):
    """更新数据源配置的请求模型。"""

    ds_type: Optional[str] = Field(None, description="数据源类型", max_length=20)
    url: Optional[str] = Field(None, description="JDBC连接地址", max_length=500)
    user: Optional[str] = Field(None, description="数据库用户名", max_length=50)
    pwd: Optional[str] = Field(None, description="数据库密码", max_length=100)
    properties: Optional[Dict[str, Any]] = Field(None, description="额外配置参数")
    updated_by: Optional[str] = Field(None, description="更新人", max_length=50)

    @validator("ds_type")
    def validate_ds_type(cls, v):
        """验证数据源类型。"""
        if v is not None:
            allowed_types = ["mysql", "hive", "postgresql", "oracle", "sqlite"]
            if v.lower() not in allowed_types:
                raise ValueError(
                    f'数据源类型必须是以下之一: {", ".join(allowed_types)}'
                )
            return v.lower()
        return v


class DataSourceConfigResponse(DataSourceConfigBase):
    """数据源配置响应模型。"""

    created_by: str = Field(..., description="创建人")
    create_time: datetime = Field(..., description="创建时间")
    updated_by: Optional[str] = Field(None, description="更新人")
    update_time: Optional[datetime] = Field(None, description="更新时间")

    class Config:
        from_attributes = True
        json_encoders = {datetime: lambda dt: dt.isoformat() if dt else None}


class DataSourceConfigList(BaseModel):
    """数据源配置列表响应模型。"""

    success: bool = True
    message: str = "获取数据源配置列表成功"
    data: List[DataSourceConfigResponse]
    total: int


class DataSourceConfigDetail(BaseModel):
    """数据源配置详情响应模型。"""

    success: bool = True
    message: str = "获取数据源配置详情成功"
    data: DataSourceConfigResponse


class DataSourceTestResult(BaseModel):
    """数据源测试结果模型。"""

    success: bool
    message: str
    latency: Optional[float] = None
    error_detail: Optional[str] = None


class APIResponse(BaseModel):
    """通用API响应模型。"""

    success: bool
    message: str
    data: Optional[Any] = None
    error: Optional[str] = None
