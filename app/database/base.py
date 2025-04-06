"""Base database operations."""
from typing import Any, Dict, List, Optional, Type, TypeVar
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from app.config import DatabaseSettings
from app.exceptions import DatabaseError

logger = logging.getLogger(__name__)

T = TypeVar('T')

class BaseDatabase:
    """基础数据库操作类"""

    def __init__(self, config: DatabaseSettings):
        """初始化数据库连接。

        Args:
            config: 数据库配置对象
        """
        self.config = config
        self.engine = create_engine(config.connection_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """执行查询SQL。

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise DatabaseError(f"Query execution failed: {str(e)}")

    def execute_write(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        return_id: bool = False
    ) -> Optional[int]:
        """执行写入SQL。

        Args:
            query: SQL写入语句
            params: 写入参数
            return_id: 是否返回新插入记录的ID

        Returns:
            如果return_id为True，返回新插入记录的ID；否则返回None

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            with self.engine.connect() as conn:
                if return_id:
                    query = f"{query} RETURNING id"
                result = conn.execute(text(query), params or {})
                conn.commit()

                if return_id:
                    return result.scalar_one()
                return None

        except SQLAlchemyError as e:
            logger.error(f"Write operation failed: {str(e)}")
            raise DatabaseError(f"Write operation failed: {str(e)}")

    def execute_many(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """批量执行SQL。

        Args:
            query: SQL语句
            params_list: 参数列表

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), params_list)
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Batch operation failed: {str(e)}")
            raise DatabaseError(f"Batch operation failed: {str(e)}")

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在。

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        try:
            with self.engine.connect() as conn:
                if self.config.driver == "postgresql":
                    query = """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = :table_name
                        )
                    """
                elif self.config.driver == "mysql":
                    query = """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = :table_name
                        )
                    """
                else:  # sqlite
                    query = """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name = :table_name
                    """

                result = conn.execute(text(query), {"table_name": table_name})
                return bool(result.scalar())
        except SQLAlchemyError as e:
            logger.error(f"Failed to check table existence: {str(e)}")
            return False
