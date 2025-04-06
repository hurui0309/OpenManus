"""Database operations for SQL review system."""
from typing import Dict, Any, Optional, List
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Config
from app.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class DatabaseOperations:
    """数据库操作类，用于处理SQL Review相关的数据库操作。"""

    def __init__(self, config: Config):
        """初始化数据库操作类。

        Args:
            config: 配置对象
        """
        self.config = config
        self.engine = create_engine(config.database.connection_url)

    def ensure_tables(self):
        """确保必要的表存在，如果不存在则创建。"""
        try:
            with self.engine.connect() as conn:
                # 检查表是否存在
                if self.config.database.driver == "postgresql":
                    result = conn.execute(text(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sql_reviews')"
                    ))
                elif "mysql" in self.config.database.driver:
                    result = conn.execute(text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :db AND table_name = 'sql_reviews')"
                    ), {"db": self.config.database.database})
                else:  # sqlite
                    result = conn.execute(text(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='sql_reviews'"
                    ))

                if not result.scalar():
                    self.create_reviews_table()
        except SQLAlchemyError as e:
            logger.error(f"Failed to check/create tables: {str(e)}")
            raise DatabaseError(f"Failed to check/create tables: {str(e)}")

    def create_reviews_table(self):
        """创建SQL Review记录表。"""
        try:
            # 根据不同数据库类型创建适当的JSON类型和自增主键
            if self.config.database.driver == "postgresql":
                json_type = "JSONB"
                id_type = "SERIAL"
            elif "mysql" in self.config.database.driver:
                json_type = "JSON"
                id_type = "INT AUTO_INCREMENT"
            else:  # sqlite
                json_type = "TEXT"
                id_type = "INTEGER PRIMARY KEY AUTOINCREMENT"

            create_table_sql = f"""
            CREATE TABLE sql_reviews (
                id {id_type},
                sql_text TEXT NOT NULL,
                review_result {json_type} NOT NULL,
                severity VARCHAR(20) NOT NULL,
                reviewer VARCHAR(50) NOT NULL,
                review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                {", PRIMARY KEY (id)" if "mysql" in self.config.database.driver else ""}
            )
            """
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))

                # 创建索引
                indexes = [
                    "CREATE INDEX idx_sql_reviews_severity ON sql_reviews (severity)",
                    "CREATE INDEX idx_sql_reviews_reviewer ON sql_reviews (reviewer)",
                    "CREATE INDEX idx_sql_reviews_review_date ON sql_reviews (review_date)"
                ]
                for index_sql in indexes:
                    conn.execute(text(index_sql))
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise DatabaseError(f"Failed to create tables: {str(e)}")

    def save_sql_review(
        self,
        sql_text: str,
        review_result: Dict[str, Any],
        severity: str,
        reviewer: str,
        review_date: Optional[str] = None
    ) -> int:
        """保存SQL Review结果到数据库。

        Args:
            sql_text: 原始SQL文本
            review_result: Review结果字典
            severity: 问题严重程度
            reviewer: 审查者（可以是人或AI）
            review_date: 审查日期（可选）

        Returns:
            review_id: 新创建的review记录ID

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            # 确保表存在
            self.ensure_tables()

            # 准备插入数据
            params = {
                "sql_text": sql_text,
                "review_result": json.dumps(review_result),
                "severity": severity,
                "reviewer": reviewer,
                "review_date": review_date or datetime.utcnow().isoformat()
            }

            # 执行插入
            with self.engine.connect() as conn:
                if "mysql" in self.config.database.driver:
                    result = conn.execute(
                        text("""
                        INSERT INTO sql_reviews (
                            sql_text, review_result, severity, reviewer, review_date
                        ) VALUES (
                            :sql_text, :review_result, :severity, :reviewer,
                            :review_date
                        )
                        """),
                        params
                    )
                    conn.commit()
                    return result.lastrowid
                else:
                    result = conn.execute(
                        text("""
                        INSERT INTO sql_reviews (
                            sql_text, review_result, severity, reviewer, review_date
                        ) VALUES (
                            :sql_text, :review_result::jsonb, :severity, :reviewer,
                            :review_date::timestamp with time zone
                        ) RETURNING id
                        """),
                        params
                    )
                    review_id = result.scalar_one()
                    conn.commit()
                    return review_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to save SQL review: {str(e)}")
            raise DatabaseError(f"Failed to save SQL review: {str(e)}")

    def get_sql_review(self, review_id: int) -> Dict[str, Any]:
        """获取指定ID的SQL Review记录。

        Args:
            review_id: Review记录ID

        Returns:
            Dict包含Review详情

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT * FROM sql_reviews WHERE id = :review_id"),
                    {"review_id": review_id}
                )
                row = result.mappings().first()
                if not row:
                    raise DatabaseError(f"Review with ID {review_id} not found")

                record = dict(row)
                record["review_result"] = json.loads(record["review_result"])
                return record

        except SQLAlchemyError as e:
            logger.error(f"Failed to get SQL review: {str(e)}")
            raise DatabaseError(f"Failed to get SQL review: {str(e)}")

    def list_sql_reviews(
        self,
        limit: int = 10,
        offset: int = 0,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """获取SQL Review记录列表。

        Args:
            limit: 返回记录数量限制
            offset: 分页偏移量
            severity: 按严重程度筛选（可选）

        Returns:
            Review记录列表

        Raises:
            DatabaseError: 当数据库操作失败时
        """
        try:
            query = "SELECT * FROM sql_reviews"
            params = {}

            if severity:
                query += " WHERE severity = :severity"
                params["severity"] = severity

            query += " ORDER BY review_date DESC LIMIT :limit OFFSET :offset"
            params.update({"limit": limit, "offset": offset})

            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                records = [dict(row) for row in result.mappings()]

                # 解析JSON结果
                for record in records:
                    record["review_result"] = json.loads(record["review_result"])

                return records

        except SQLAlchemyError as e:
            logger.error(f"Failed to list SQL reviews: {str(e)}")
            raise DatabaseError(f"Failed to list SQL reviews: {str(e)}")
