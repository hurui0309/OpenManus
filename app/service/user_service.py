"""
用户管理服务
提供用户注册、登录、密码管理等功能
"""

import hashlib
import logging
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.config import Config
from app.schemas.user import (
    LoginResponse,
    UserCreate,
    UserLogin,
    UserResponse,
    UserUpdate,
)

logger = logging.getLogger(__name__)


class UserService:
    """用户管理服务类"""

    @staticmethod
    def _get_engine():
        """获取数据库引擎"""
        config = Config()
        return create_engine(config.database.connection_url)

    @staticmethod
    def _hash_password(password: str) -> str:
        """对密码进行哈希加密"""
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    @staticmethod
    def _verify_password(plain_password: str, hashed_password: str) -> bool:
        """验证密码"""
        return UserService._hash_password(plain_password) == hashed_password

    @staticmethod
    def create_user(user_data: UserCreate) -> UserResponse:
        """
        创建新用户

        Args:
            user_data: 用户创建数据

        Returns:
            UserResponse: 创建的用户信息

        Raises:
            ValueError: 用户名已存在或数据无效
            Exception: 数据库操作异常
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                # 检查用户名是否已存在
                check_sql = text(
                    """
                    SELECT id FROM t_users WHERE username = :username
                """
                )
                result = conn.execute(check_sql, {"username": user_data.username})
                if result.fetchone():
                    raise ValueError(f"用户名 '{user_data.username}' 已存在")

                # 哈希密码
                password_hash = UserService._hash_password(user_data.password)

                # 插入新用户
                insert_sql = text(
                    """
                    INSERT INTO t_users (username, password_hash)
                    VALUES (:username, :password_hash)
                """
                )
                result = conn.execute(
                    insert_sql,
                    {"username": user_data.username, "password_hash": password_hash},
                )

                user_id = result.lastrowid

                # 获取完整用户信息
                get_sql = text(
                    """
                    SELECT id, username, is_active, created_time, updated_time
                    FROM t_users WHERE id = :user_id
                """
                )
                user_row = conn.execute(get_sql, {"user_id": user_id}).fetchone()

                if not user_row:
                    raise Exception("创建用户后无法获取用户信息")

                return UserResponse(
                    id=user_row.id,
                    username=user_row.username,
                    is_active=bool(user_row.is_active),
                    created_time=user_row.created_time,
                    updated_time=user_row.updated_time,
                )

        except IntegrityError as e:
            logger.error(f"创建用户失败，数据完整性错误: {e}")
            raise ValueError("用户名已存在或数据格式错误")
        except SQLAlchemyError as e:
            logger.error(f"创建用户时数据库错误: {e}")
            raise Exception("数据库操作失败")
        except Exception as e:
            logger.error(f"创建用户时发生未知错误: {e}")
            raise

    @staticmethod
    def authenticate_user(login_data: UserLogin) -> LoginResponse:
        """
        用户登录认证

        Args:
            login_data: 登录数据

        Returns:
            LoginResponse: 登录响应信息

        Raises:
            ValueError: 用户名或密码错误
            Exception: 数据库操作异常
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                # 查询用户信息
                sql = text(
                    """
                    SELECT id, username, password_hash, is_active, created_time, updated_time
                    FROM t_users
                    WHERE username = :username
                """
                )
                user_row = conn.execute(
                    sql, {"username": login_data.username}
                ).fetchone()

                if not user_row:
                    raise ValueError("用户名或密码错误")

                # 验证密码
                if not UserService._verify_password(
                    login_data.password, user_row.password_hash
                ):
                    raise ValueError("用户名或密码错误")

                # 检查用户状态
                if not user_row.is_active:
                    raise ValueError("用户账号已被禁用")

                user_info = UserResponse(
                    id=user_row.id,
                    username=user_row.username,
                    is_active=bool(user_row.is_active),
                    created_time=user_row.created_time,
                    updated_time=user_row.updated_time,
                )

                return LoginResponse(user=user_info, message="登录成功")

        except ValueError:
            raise
        except SQLAlchemyError as e:
            logger.error(f"用户登录时数据库错误: {e}")
            raise Exception("数据库操作失败")
        except Exception as e:
            logger.error(f"用户登录时发生未知错误: {e}")
            raise

    @staticmethod
    def get_user_by_id(user_id: int) -> Optional[UserResponse]:
        """
        根据用户ID获取用户信息

        Args:
            user_id: 用户ID

        Returns:
            Optional[UserResponse]: 用户信息，不存在则返回None
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                sql = text(
                    """
                    SELECT id, username, is_active, created_time, updated_time
                    FROM t_users
                    WHERE id = :user_id
                """
                )
                user_row = conn.execute(sql, {"user_id": user_id}).fetchone()

                if not user_row:
                    return None

                return UserResponse(
                    id=user_row.id,
                    username=user_row.username,
                    is_active=bool(user_row.is_active),
                    created_time=user_row.created_time,
                    updated_time=user_row.updated_time,
                )

        except SQLAlchemyError as e:
            logger.error(f"获取用户信息时数据库错误: {e}")
            raise Exception("数据库操作失败")

    @staticmethod
    def get_user_by_username(username: str) -> Optional[UserResponse]:
        """
        根据用户名获取用户信息

        Args:
            username: 用户名

        Returns:
            Optional[UserResponse]: 用户信息，不存在则返回None
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                sql = text(
                    """
                    SELECT id, username, is_active, created_time, updated_time
                    FROM t_users
                    WHERE username = :username
                """
                )
                user_row = conn.execute(sql, {"username": username}).fetchone()

                if not user_row:
                    return None

                return UserResponse(
                    id=user_row.id,
                    username=user_row.username,
                    is_active=bool(user_row.is_active),
                    created_time=user_row.created_time,
                    updated_time=user_row.updated_time,
                )

        except SQLAlchemyError as e:
            logger.error(f"获取用户信息时数据库错误: {e}")
            raise Exception("数据库操作失败")

    @staticmethod
    def update_user(user_id: int, update_data: UserUpdate) -> UserResponse:
        """
        更新用户信息

        Args:
            user_id: 用户ID
            update_data: 更新数据

        Returns:
            UserResponse: 更新后的用户信息

        Raises:
            ValueError: 用户不存在
            Exception: 数据库操作异常
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                # 检查用户是否存在
                check_sql = text("SELECT id FROM t_users WHERE id = :user_id")
                if not conn.execute(check_sql, {"user_id": user_id}).fetchone():
                    raise ValueError(f"用户ID {user_id} 不存在")

                # 构建更新SQL
                update_fields = []
                params = {"user_id": user_id}

                if update_data.password is not None:
                    update_fields.append("password_hash = :password_hash")
                    params["password_hash"] = UserService._hash_password(
                        update_data.password
                    )

                if update_data.is_active is not None:
                    update_fields.append("is_active = :is_active")
                    params["is_active"] = update_data.is_active

                if not update_fields:
                    # 没有需要更新的字段，直接返回用户信息
                    return UserService.get_user_by_id(user_id)

                # 执行更新
                update_sql = text(
                    f"""
                    UPDATE t_users
                    SET {', '.join(update_fields)}
                    WHERE id = :user_id
                """
                )
                conn.execute(update_sql, params)

                # 返回更新后的用户信息
                return UserService.get_user_by_id(user_id)

        except ValueError:
            raise
        except SQLAlchemyError as e:
            logger.error(f"更新用户信息时数据库错误: {e}")
            raise Exception("数据库操作失败")
        except Exception as e:
            logger.error(f"更新用户信息时发生未知错误: {e}")
            raise

    @staticmethod
    def list_users(skip: int = 0, limit: int = 100) -> List[UserResponse]:
        """
        获取用户列表

        Args:
            skip: 跳过的记录数
            limit: 返回的记录数限制

        Returns:
            List[UserResponse]: 用户列表
        """
        engine = UserService._get_engine()

        try:
            with engine.begin() as conn:
                sql = text(
                    """
                    SELECT id, username, is_active, created_time, updated_time
                    FROM t_users
                    ORDER BY created_time DESC
                    LIMIT :limit OFFSET :skip
                """
                )

                rows = conn.execute(sql, {"skip": skip, "limit": limit}).fetchall()

                return [
                    UserResponse(
                        id=row.id,
                        username=row.username,
                        is_active=bool(row.is_active),
                        created_time=row.created_time,
                        updated_time=row.updated_time,
                    )
                    for row in rows
                ]

        except SQLAlchemyError as e:
            logger.error(f"获取用户列表时数据库错误: {e}")
            raise Exception("数据库操作失败")
