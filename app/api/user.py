"""
用户管理API接口
提供用户注册、登录、信息管理等功能
"""

import logging
from typing import List

from fastapi import APIRouter, HTTPException, Query, status

from app.schemas.user import (
    LoginResponse,
    UserCreate,
    UserLogin,
    UserResponse,
    UserUpdate,
)
from app.service.user_service import UserService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/users", tags=["用户管理"])


@router.post("/register", response_model=UserResponse, summary="用户注册")
async def register_user(user_data: UserCreate):
    """
    用户注册接口

    Args:
        user_data: 用户注册数据

    Returns:
        UserResponse: 注册成功的用户信息

    Raises:
        HTTPException: 用户名已存在或其他错误
    """
    try:
        user = UserService.create_user(user_data)
        logger.info(f"用户注册成功: {user.username}")
        return user
    except ValueError as e:
        logger.warning(f"用户注册失败: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"用户注册时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="注册失败，请稍后重试",
        )


@router.post("/login", response_model=LoginResponse, summary="用户登录")
async def login_user(login_data: UserLogin):
    """
    用户登录接口

    Args:
        login_data: 用户登录数据

    Returns:
        LoginResponse: 登录成功响应

    Raises:
        HTTPException: 用户名或密码错误
    """
    try:
        result = UserService.authenticate_user(login_data)
        logger.info(f"用户登录成功: {result.user.username}")
        return result
    except ValueError as e:
        logger.warning(f"用户登录失败: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
    except Exception as e:
        logger.error(f"用户登录时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="登录失败，请稍后重试",
        )


@router.get("/{user_id}", response_model=UserResponse, summary="获取用户信息")
async def get_user(user_id: int):
    """
    根据用户ID获取用户信息

    Args:
        user_id: 用户ID

    Returns:
        UserResponse: 用户信息

    Raises:
        HTTPException: 用户不存在
    """
    try:
        user = UserService.get_user_by_id(user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"用户ID {user_id} 不存在"
            )
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户信息时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="获取用户信息失败"
        )


@router.get(
    "/username/{username}",
    response_model=UserResponse,
    summary="根据用户名获取用户信息",
)
async def get_user_by_username(username: str):
    """
    根据用户名获取用户信息

    Args:
        username: 用户名

    Returns:
        UserResponse: 用户信息

    Raises:
        HTTPException: 用户不存在
    """
    try:
        user = UserService.get_user_by_username(username)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"用户名 '{username}' 不存在",
            )
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户信息时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="获取用户信息失败"
        )


@router.put("/{user_id}", response_model=UserResponse, summary="更新用户信息")
async def update_user(user_id: int, update_data: UserUpdate):
    """
    更新用户信息

    Args:
        user_id: 用户ID
        update_data: 更新数据

    Returns:
        UserResponse: 更新后的用户信息

    Raises:
        HTTPException: 用户不存在或更新失败
    """
    try:
        user = UserService.update_user(user_id, update_data)
        logger.info(f"用户信息更新成功: user_id={user_id}")
        return user
    except ValueError as e:
        logger.warning(f"更新用户信息失败: {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"更新用户信息时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="更新用户信息失败"
        )


@router.get("/", response_model=List[UserResponse], summary="获取用户列表")
async def list_users(
    skip: int = Query(0, ge=0, description="跳过的记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回的记录数限制"),
):
    """
    获取用户列表

    Args:
        skip: 跳过的记录数
        limit: 返回的记录数限制

    Returns:
        List[UserResponse]: 用户列表
    """
    try:
        users = UserService.list_users(skip=skip, limit=limit)
        logger.info(f"获取用户列表成功: 返回 {len(users)} 条记录")
        return users
    except Exception as e:
        logger.error(f"获取用户列表时发生错误: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="获取用户列表失败"
        )
