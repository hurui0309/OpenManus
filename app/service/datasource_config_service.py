"""数据源配置服务层。"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.config import Config
from app.datasource.manager import DataSourceConfig, DataSourceManager
from app.exceptions import DatabaseError
from app.schemas.datasource import (
    DataSourceConfigCreate,
    DataSourceConfigResponse,
    DataSourceConfigUpdate,
)

logger = logging.getLogger(__name__)


class DataSourceConfigService:
    """数据源配置服务类。"""

    def __init__(self, config: Config):
        self.config = config
        self.datasource_manager = DataSourceManager(config)

    async def create_datasource_config(
        self, config_data: DataSourceConfigCreate
    ) -> DataSourceConfigResponse:
        """创建数据源配置。

        Args:
            config_data: 数据源配置创建数据

        Returns:
            DataSourceConfigResponse: 创建的数据源配置

        Raises:
            DatabaseError: 当创建失败时
        """
        try:
            # 检查数据源名称是否已存在
            existing = await self._get_datasource_by_name(config_data.ds_name)
            if existing:
                raise DatabaseError(f"数据源名称 '{config_data.ds_name}' 已存在")

            # 准备properties JSON字符串
            properties_json = json.dumps(config_data.properties, ensure_ascii=False)

            # 插入数据源配置
            insert_sql = """
                INSERT INTO t_datasource_config (
                    ds_name, ds_type, url, user, pwd, properties,
                    created_by, create_time
                ) VALUES (
                    :ds_name, :ds_type, :url, :user, :pwd, :properties,
                    :created_by, NOW()
                )
            """

            with self.datasource_manager.main_engine.begin() as conn:
                conn.execute(
                    text(insert_sql),
                    {
                        "ds_name": config_data.ds_name,
                        "ds_type": config_data.ds_type,
                        "url": config_data.url,
                        "user": config_data.user,
                        "pwd": config_data.pwd,
                        "properties": properties_json,
                        "created_by": config_data.created_by,
                    },
                )

            # 清除缓存
            self._clear_cache(config_data.ds_name)

            # 获取并返回创建的配置
            result = await self._get_datasource_by_name(config_data.ds_name)
            logger.info(
                f"数据源配置 '{config_data.ds_name}' 创建成功，创建人: {config_data.created_by}"
            )
            return result

        except IntegrityError as e:
            logger.error(f"数据源配置创建失败，违反唯一性约束: {str(e)}")
            raise DatabaseError(f"数据源名称 '{config_data.ds_name}' 已存在")
        except SQLAlchemyError as e:
            logger.error(f"数据源配置创建失败: {str(e)}")
            raise DatabaseError(f"创建数据源配置失败: {str(e)}")

    async def get_datasource_config(
        self, ds_name: str
    ) -> Optional[DataSourceConfigResponse]:
        """获取指定数据源配置。

        Args:
            ds_name: 数据源名称

        Returns:
            DataSourceConfigResponse: 数据源配置，如果不存在则返回None
        """
        return await self._get_datasource_by_name(ds_name)

    async def list_datasource_configs(
        self,
        created_by: Optional[str] = None,
        ds_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """获取数据源配置列表。

        Args:
            created_by: 创建人过滤条件
            ds_type: 数据源类型过滤条件
            page: 页码
            page_size: 每页大小

        Returns:
            包含数据源配置列表和总数的字典
        """
        try:
            # 构建查询条件
            where_conditions = []
            params = {}

            if created_by:
                where_conditions.append("created_by = :created_by")
                params["created_by"] = created_by

            if ds_type:
                where_conditions.append("ds_type = :ds_type")
                params["ds_type"] = ds_type

            where_clause = (
                "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            )

            # 计算偏移量
            offset = (page - 1) * page_size
            params.update({"limit": page_size, "offset": offset})

            # 查询总数
            count_sql = f"""
                SELECT COUNT(*) as total
                FROM t_datasource_config
                {where_clause}
            """

            # 查询数据
            list_sql = f"""
                SELECT ds_name, ds_type, url, user, pwd, properties,
                       created_by, create_time, updated_by, update_time
                FROM t_datasource_config
                {where_clause}
                ORDER BY create_time DESC
                LIMIT :limit OFFSET :offset
            """

            with self.datasource_manager.main_engine.connect() as conn:
                # 获取总数
                total_result = conn.execute(text(count_sql), params)
                total = total_result.scalar()

                # 获取数据
                result = conn.execute(text(list_sql), params)
                rows = result.fetchall()

            # 转换为响应模型
            configs = []
            for row in rows:
                properties = {}
                if row.properties:
                    try:
                        properties = json.loads(row.properties)
                    except json.JSONDecodeError:
                        logger.warning(f"数据源 {row.ds_name} 的properties字段格式错误")

                config_response = DataSourceConfigResponse(
                    ds_name=row.ds_name,
                    ds_type=row.ds_type,
                    url=row.url,
                    user=row.user,
                    pwd=row.pwd,
                    properties=properties,
                    created_by=row.created_by,
                    create_time=row.create_time,
                    updated_by=row.updated_by,
                    update_time=row.update_time,
                )
                configs.append(config_response)

            return {
                "configs": configs,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size,
            }

        except SQLAlchemyError as e:
            logger.error(f"获取数据源配置列表失败: {str(e)}")
            raise DatabaseError(f"获取数据源配置列表失败: {str(e)}")

    async def update_datasource_config(
        self, ds_name: str, config_data: DataSourceConfigUpdate, current_user: str
    ) -> DataSourceConfigResponse:
        """更新数据源配置。

        Args:
            ds_name: 数据源名称
            config_data: 更新数据
            current_user: 当前操作用户

        Returns:
            DataSourceConfigResponse: 更新后的数据源配置

        Raises:
            DatabaseError: 当数据源不存在或更新失败时
        """
        try:
            # 检查数据源是否存在
            existing = await self._get_datasource_by_name(ds_name)
            if not existing:
                raise DatabaseError(f"数据源 '{ds_name}' 不存在")

            # 构建更新SQL
            update_fields = []
            params = {"ds_name": ds_name, "updated_by": current_user}

            if config_data.ds_type is not None:
                update_fields.append("ds_type = :ds_type")
                params["ds_type"] = config_data.ds_type

            if config_data.url is not None:
                update_fields.append("url = :url")
                params["url"] = config_data.url

            if config_data.user is not None:
                update_fields.append("user = :user")
                params["user"] = config_data.user

            if config_data.pwd is not None:
                update_fields.append("pwd = :pwd")
                params["pwd"] = config_data.pwd

            if config_data.properties is not None:
                update_fields.append("properties = :properties")
                params["properties"] = json.dumps(
                    config_data.properties, ensure_ascii=False
                )

            if not update_fields:
                # 没有要更新的字段，直接返回当前配置
                return existing

            update_fields.extend(["updated_by = :updated_by", "update_time = NOW()"])

            update_sql = f"""
                UPDATE t_datasource_config
                SET {', '.join(update_fields)}
                WHERE ds_name = :ds_name
            """

            with self.datasource_manager.main_engine.begin() as conn:
                result = conn.execute(text(update_sql), params)
                if result.rowcount == 0:
                    raise DatabaseError(f"数据源 '{ds_name}' 不存在")

            # 清除缓存
            self._clear_cache(ds_name)

            # 获取并返回更新后的配置
            result = await self._get_datasource_by_name(ds_name)
            logger.info(f"数据源配置 '{ds_name}' 更新成功，操作人: {current_user}")
            return result

        except SQLAlchemyError as e:
            logger.error(f"数据源配置更新失败: {str(e)}")
            raise DatabaseError(f"更新数据源配置失败: {str(e)}")

    async def delete_datasource_config(self, ds_name: str, current_user: str) -> bool:
        """删除数据源配置。

        Args:
            ds_name: 数据源名称
            current_user: 当前操作用户

        Returns:
            bool: 是否删除成功

        Raises:
            DatabaseError: 当数据源不存在、无权限或删除失败时
        """
        try:
            # 检查数据源是否存在
            existing = await self._get_datasource_by_name(ds_name)
            if not existing:
                raise DatabaseError(f"数据源 '{ds_name}' 不存在")

            # 检查删除权限：只有创建人才能删除
            if existing.created_by != current_user:
                raise DatabaseError(
                    f"无权限删除数据源 '{ds_name}'，只有创建人 '{existing.created_by}' 才能删除"
                )

            # 删除数据源配置
            delete_sql = """
                DELETE FROM t_datasource_config
                WHERE ds_name = :ds_name AND created_by = :created_by
            """

            with self.datasource_manager.main_engine.begin() as conn:
                result = conn.execute(
                    text(delete_sql), {"ds_name": ds_name, "created_by": current_user}
                )
                if result.rowcount == 0:
                    raise DatabaseError(f"删除失败，数据源 '{ds_name}' 不存在或无权限")

            # 清除缓存
            self._clear_cache(ds_name)

            logger.info(f"数据源配置 '{ds_name}' 删除成功，操作人: {current_user}")
            return True

        except SQLAlchemyError as e:
            logger.error(f"数据源配置删除失败: {str(e)}")
            raise DatabaseError(f"删除数据源配置失败: {str(e)}")

    async def test_datasource_connection(self, ds_name: str) -> Dict[str, Any]:
        """测试数据源连接。

        Args:
            ds_name: 数据源名称

        Returns:
            包含测试结果的字典
        """
        try:
            # 检查数据源是否存在
            existing = await self._get_datasource_by_name(ds_name)
            if not existing:
                return {
                    "success": False,
                    "message": f"数据源 '{ds_name}' 不存在",
                    "latency": None,
                    "error_detail": f"数据源 '{ds_name}' 不存在",
                }

            # 测试连接
            import time

            start_time = time.time()

            success = await self.datasource_manager.test_connection(ds_name)

            latency = round((time.time() - start_time) * 1000, 2)  # 转换为毫秒

            if success:
                return {
                    "success": True,
                    "message": f"数据源 '{ds_name}' 连接成功",
                    "latency": latency,
                    "error_detail": None,
                }
            else:
                return {
                    "success": False,
                    "message": f"数据源 '{ds_name}' 连接失败",
                    "latency": latency,
                    "error_detail": "连接测试失败，请检查配置参数",
                }

        except Exception as e:
            logger.error(f"测试数据源连接失败: {str(e)}")
            return {
                "success": False,
                "message": f"测试数据源连接失败: {str(e)}",
                "latency": None,
                "error_detail": str(e),
            }

    async def _get_datasource_by_name(
        self, ds_name: str
    ) -> Optional[DataSourceConfigResponse]:
        """根据名称获取数据源配置。

        Args:
            ds_name: 数据源名称

        Returns:
            DataSourceConfigResponse: 数据源配置，如果不存在则返回None
        """
        try:
            sql = """
                SELECT ds_name, ds_type, url, user, pwd, properties,
                       created_by, create_time, updated_by, update_time
                FROM t_datasource_config
                WHERE ds_name = :ds_name
            """

            with self.datasource_manager.main_engine.connect() as conn:
                result = conn.execute(text(sql), {"ds_name": ds_name})
                row = result.fetchone()

                if not row:
                    return None

                properties = {}
                if row.properties:
                    try:
                        properties = json.loads(row.properties)
                    except json.JSONDecodeError:
                        logger.warning(f"数据源 {ds_name} 的properties字段格式错误")

                return DataSourceConfigResponse(
                    ds_name=row.ds_name,
                    ds_type=row.ds_type,
                    url=row.url,
                    user=row.user,
                    pwd=row.pwd,
                    properties=properties,
                    created_by=row.created_by,
                    create_time=row.create_time,
                    updated_by=row.updated_by,
                    update_time=row.update_time,
                )

        except SQLAlchemyError as e:
            logger.error(f"获取数据源配置失败: {str(e)}")
            raise DatabaseError(f"获取数据源配置失败: {str(e)}")

    def _clear_cache(self, ds_name: str):
        """清除指定数据源的缓存。

        Args:
            ds_name: 数据源名称
        """
        # 清除DataSourceManager中的缓存
        if ds_name in self.datasource_manager._configs:
            del self.datasource_manager._configs[ds_name]

        if ds_name in self.datasource_manager._engines:
            # 关闭引擎连接
            self.datasource_manager._engines[ds_name].dispose()
            del self.datasource_manager._engines[ds_name]

        logger.debug(f"已清除数据源 '{ds_name}' 的缓存")
