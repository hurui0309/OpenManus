"""
用户相关的数据模型定义
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    """用户注册模型"""

    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")


class UserLogin(BaseModel):
    """用户登录模型"""

    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")


class UserResponse(BaseModel):
    """用户响应模型"""

    id: int = Field(..., description="用户ID")
    username: str = Field(..., description="用户名")
    is_active: bool = Field(..., description="是否激活")
    created_time: datetime = Field(..., description="创建时间")
    updated_time: datetime = Field(..., description="更新时间")

    class Config:
        from_attributes = True


class UserUpdate(BaseModel):
    """用户更新模型"""

    password: Optional[str] = Field(
        None, min_length=6, max_length=128, description="新密码"
    )
    is_active: Optional[bool] = Field(None, description="是否激活")


class LoginResponse(BaseModel):
    """登录响应模型"""

    user: UserResponse = Field(..., description="用户信息")
    message: str = Field(..., description="响应消息")
