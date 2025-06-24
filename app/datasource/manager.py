"""数据源管理器，用于管理多种数据源连接。"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import Config
from app.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DataSourceConfig:
    """数据源配置类。"""

    # 时区名称到UTC偏移量的映射
    TIMEZONE_MAPPING = {
        "Asia/Shanghai": "+08:00",
        "Asia/Beijing": "+08:00",
        "Asia/Hong_Kong": "+08:00",
        "Asia/Tokyo": "+09:00",
        "Asia/Seoul": "+09:00",
        "Europe/London": "+00:00",
        "Europe/Paris": "+01:00",
        "America/New_York": "-05:00",
        "America/Los_Angeles": "-08:00",
        "UTC": "+00:00",
        "GMT": "+00:00",
    }

    def __init__(
        self,
        ds_name: str,
        ds_type: str,
        url: str,
        user: str,
        pwd: str,
        properties: Optional[Dict[str, Any]] = None,
    ):
        self.ds_name = ds_name
        self.ds_type = ds_type
        self.url = url
        self.user = user
        self.pwd = pwd
        self.properties = properties or {}

    def get_connection_url(self) -> str:
        """获取SQLAlchemy连接URL。

        Returns:
            str: SQLAlchemy格式的连接URL

        Raises:
            ValueError: 当URL格式无效或数据源类型不支持时
        """
        try:
            # 检查是否为JDBC URL格式
            if self.url.startswith("jdbc:"):
                return self._parse_jdbc_url()
            else:
                # 处理旧格式（向后兼容）
                return self._get_legacy_connection_url()
        except Exception as e:
            logger.error(f"解析数据源 '{self.ds_name}' 连接URL失败: {str(e)}")
            raise ValueError(f"解析连接URL失败: {str(e)}")

    def _convert_timezone_to_offset(self, timezone_name: str) -> str:
        """将时区名称转换为UTC偏移量格式。

        Args:
            timezone_name: 时区名称，如'Asia/Shanghai'或URL编码的偏移量'%2B08:00'

        Returns:
            str: UTC偏移量格式，如'+08:00'
        """
        # 首先进行URL解码
        decoded_timezone = unquote(timezone_name)

        if decoded_timezone in self.TIMEZONE_MAPPING:
            return self.TIMEZONE_MAPPING[decoded_timezone]

        # 如果已经是偏移量格式，直接返回
        if re.match(r"^[+-]\d{2}:\d{2}$", decoded_timezone):
            return decoded_timezone

        # 默认返回UTC
        logger.warning(f"未知时区 '{decoded_timezone}'，使用UTC作为默认值")
        return "+00:00"

    def get_engine_kwargs(self) -> Dict[str, Any]:
        """获取创建引擎的额外参数。

        Returns:
            Dict[str, Any]: 包含connect_args等参数的字典
        """
        engine_kwargs = {}

        if self.ds_type.lower() == "mysql":
            connect_args = {}

            # 从URL参数中提取MySQL特定的连接参数
            if "?" in self.url:
                url_part, params_part = self.url.split("?", 1)
                for param in params_part.split("&"):
                    if "=" in param:
                        key, value = param.split("=", 1)
                        if key == "characterEncoding":
                            # URL解码字符编码参数
                            connect_args["charset"] = unquote(value)
                        elif key == "allowMultiQueries":
                            connect_args["autocommit"] = value.lower() == "true"
                        elif key == "useSSL":
                            connect_args["ssl_disabled"] = value.lower() != "true"
                        elif key == "serverTimezone":
                            # 转换时区名称为UTC偏移量格式（支持URL编码）
                            timezone_offset = self._convert_timezone_to_offset(value)
                            connect_args["init_command"] = (
                                f"SET time_zone = '{timezone_offset}'"
                            )

            # 从properties中获取额外的连接参数
            if self.properties:
                if "charset" in self.properties:
                    connect_args["charset"] = self.properties["charset"]
                if "autocommit" in self.properties:
                    connect_args["autocommit"] = self.properties["autocommit"]
                if "timezone" in self.properties:
                    # 处理properties中的时区设置
                    timezone_offset = self._convert_timezone_to_offset(
                        self.properties["timezone"]
                    )
                    connect_args["init_command"] = (
                        f"SET time_zone = '{timezone_offset}'"
                    )

            if connect_args:
                engine_kwargs["connect_args"] = connect_args

        elif self.ds_type.lower() == "hive":
            # Hive连接配置
            connect_args = {}

            # 从properties中获取连接参数，如果没有则使用默认配置
            if self.properties:
                # 支持其他Hive连接参数
                for key in [
                    "kerberos_service_name",
                    "thrift_transport",
                    "configuration",
                ]:
                    if key in self.properties:
                        connect_args[key] = self.properties[key]

            # 对于Python 3.12，确保使用pure-sasl兼容的配置
            import sys

            if sys.version_info >= (3, 12):
                # Python 3.12需要特殊处理，确保使用pure-sasl
                try:
                    import puretransport

                    # 如果安装了pure-transport，使用它作为thrift_transport
                    from puretransport import PureSASLHTTPThriftTransport

                    connect_args["thrift_transport"] = PureSASLHTTPThriftTransport
                except ImportError:
                    # 如果没有pure-transport，依赖pure-sasl的默认处理
                    pass

            if connect_args:
                engine_kwargs["connect_args"] = connect_args

            # 添加池化和超时配置 - 针对Hive优化
            engine_kwargs.update(
                {
                    "pool_size": 3,  # 增加连接池大小
                    "max_overflow": 2,  # 允许额外连接
                    "pool_timeout": 60,  # 增加超时时间
                    "pool_recycle": 1800,  # 30分钟回收连接
                    "pool_pre_ping": True,  # 连接前ping测试
                }
            )

        return engine_kwargs

    def _parse_jdbc_url(self) -> str:
        """解析JDBC URL格式。"""
        if self.url.startswith("jdbc:mysql://"):
            return self._parse_mysql_jdbc_url()
        elif self.url.startswith("jdbc:hive2://"):
            return self._parse_hive_jdbc_url()
        elif self.url.startswith("jdbc:postgresql://"):
            return self._parse_postgresql_jdbc_url()
        elif self.url.startswith("jdbc:oracle:"):
            return self._parse_oracle_jdbc_url()
        else:
            raise ValueError(f"不支持的数据源类型: {self.ds_type}")

    def _parse_mysql_jdbc_url(self) -> str:
        """解析MySQL JDBC URL。
        从 jdbc:mysql://host:port/database?params 转换为 mysql+pymysql://user:pwd@host:port/database
        注意：参数如charset、autocommit等将通过connect_args传递，不包含在URL中
        """
        # 移除 jdbc:mysql:// 前缀
        url_without_prefix = self.url.replace("jdbc:mysql://", "")

        # 分离主机端口数据库和参数
        if "?" in url_without_prefix:
            host_db_part, params_part = url_without_prefix.split("?", 1)
        else:
            host_db_part = url_without_prefix

        # 解析主机、端口和数据库
        parts = host_db_part.split("/")
        if len(parts) != 2:
            raise ValueError(f"无效的MySQL JDBC URL格式: {self.url}")

        host_port = parts[0]
        database = parts[1]

        # 构建纯净的SQLAlchemy MySQL URL（不包含特殊参数）
        return f"mysql+pymysql://{self.user}:{self.pwd}@{host_port}/{database}"

    def _parse_hive_jdbc_url(self) -> str:
        """解析Hive JDBC URL。
        从 jdbc:hive2://host:port/database;params 转换为 hive://user@host:port/database?auth=NONE
        """
        # 移除 jdbc:hive2:// 前缀
        url_without_prefix = self.url.replace("jdbc:hive2://", "")

        # 分离主机端口数据库和参数（Hive使用分号分隔参数）
        if ";" in url_without_prefix:
            host_db_part, params_part = url_without_prefix.split(";", 1)
        else:
            host_db_part = url_without_prefix

        # 解析主机、端口和数据库
        parts = host_db_part.split("/")
        if len(parts) != 2:
            raise ValueError(f"无效的Hive JDBC URL格式: {self.url}")

        host_port = parts[0]
        database = parts[1]

        # 获取认证模式
        auth_mode = "NONE"  # 默认使用NONE认证
        if self.properties and "auth" in self.properties:
            auth_mode = self.properties["auth"]

        # 根据测试结果，使用hive作为默认用户名
        username = self.user if self.user else "hive"

        # 构建SQLAlchemy Hive URL
        if auth_mode.upper() in ["NONE", "NOSASL"]:
            # NONE/NOSASL模式使用hive用户名，不使用密码
            return f"hive://{username}@{host_port}/{database}?auth={auth_mode}"
        else:
            # 其他认证模式需要用户名密码
            return (
                f"hive://{self.user}:{self.pwd}@{host_port}/{database}?auth={auth_mode}"
            )

    def _parse_postgresql_jdbc_url(self) -> str:
        """解析PostgreSQL JDBC URL。"""
        # 移除 jdbc:postgresql:// 前缀
        url_without_prefix = self.url.replace("jdbc:postgresql://", "")

        # 分离主机端口数据库和参数
        if "?" in url_without_prefix:
            host_db_part, params_part = url_without_prefix.split("?", 1)
        else:
            host_db_part = url_without_prefix

        # 解析主机、端口和数据库
        parts = host_db_part.split("/")
        if len(parts) != 2:
            raise ValueError(f"无效的PostgreSQL JDBC URL格式: {self.url}")

        host_port = parts[0]
        database = parts[1]

        return f"postgresql://{self.user}:{self.pwd}@{host_port}/{database}"

    def _parse_oracle_jdbc_url(self) -> str:
        """解析Oracle JDBC URL。"""
        if "jdbc:oracle:thin:@//" in self.url:
            # 服务名格式: jdbc:oracle:thin:@//host:port/service
            url_without_prefix = self.url.replace("jdbc:oracle:thin:@//", "")
            return f"oracle+cx_oracle://{self.user}:{self.pwd}@{url_without_prefix}"
        elif "jdbc:oracle:thin:@" in self.url:
            # SID格式: jdbc:oracle:thin:@host:port:sid
            url_without_prefix = self.url.replace("jdbc:oracle:thin:@", "")
            return f"oracle+cx_oracle://{self.user}:{self.pwd}@{url_without_prefix}"
        else:
            raise ValueError(f"无效的Oracle JDBC URL格式: {self.url}")

    def _get_legacy_connection_url(self) -> str:
        """处理旧格式的连接URL（向后兼容）。"""
        if self.ds_type.lower() == "mysql":
            return f"mysql+pymysql://{self.user}:{self.pwd}@{self.url}"
        elif self.ds_type.lower() == "hive":
            # 对于Hive，使用测试成功的配置
            username = self.user if self.user else "hive"
            auth_mode = "NONE"
            if self.properties and "auth" in self.properties:
                auth_mode = self.properties["auth"]
            return f"hive://{username}@{self.url}?auth={auth_mode}"
        elif self.ds_type.lower() == "oracle":
            return f"oracle+cx_oracle://{self.user}:{self.pwd}@{self.url}"
        elif self.ds_type.lower() == "postgresql":
            return f"postgresql://{self.user}:{self.pwd}@{self.url}"
        elif self.ds_type.lower() == "sqlite":
            return f"sqlite:///{self.url}"
        else:
            raise ValueError(f"不支持的数据源类型: {self.ds_type}")


class DataSourceManager:
    """数据源管理器。"""

    def __init__(self, config: Config):
        self.config = config
        self._engines: Dict[str, Engine] = {}
        self._configs: Dict[str, DataSourceConfig] = {}

        # 主数据库引擎（用于查询数据源配置表） - 使用配置库
        self.main_engine = create_engine(config.database.connection_url)

    async def get_datasource_config(self, ds_name: str) -> DataSourceConfig:
        """从配置数据库获取数据源配置。

        Args:
            ds_name: 数据源名称

        Returns:
            DataSourceConfig: 数据源配置对象

        Raises:
            DatabaseError: 当数据源不存在或配置错误时
        """
        if ds_name in self._configs:
            return self._configs[ds_name]

        try:
            with self.main_engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT ds_name, ds_type, url, user, pwd, properties "
                        "FROM t_datasource_config WHERE ds_name = :ds_name"
                    ),
                    {"ds_name": ds_name},
                )
                row = result.fetchone()

                if not row:
                    raise DatabaseError(f"数据源 '{ds_name}' 不存在")

                # 解析properties字段
                properties = {}
                if row.properties:
                    try:
                        properties = json.loads(row.properties)
                    except json.JSONDecodeError:
                        logger.warning(f"数据源 {ds_name} 的properties字段格式错误")

                config = DataSourceConfig(
                    ds_name=row.ds_name,
                    ds_type=row.ds_type,
                    url=row.url,
                    user=row.user,
                    pwd=row.pwd,
                    properties=properties,
                )

                # 缓存配置
                self._configs[ds_name] = config
                return config

        except SQLAlchemyError as e:
            logger.error(f"获取数据源配置失败: {str(e)}")
            raise DatabaseError(f"获取数据源配置失败: {str(e)}")

    async def get_engine(self, ds_name: str) -> Engine:
        """获取指定数据源的数据库引擎。

        Args:
            ds_name: 数据源名称

        Returns:
            Engine: SQLAlchemy引擎对象

        Raises:
            DatabaseError: 当创建引擎失败时
        """
        if ds_name in self._engines:
            return self._engines[ds_name]

        try:
            config = await self.get_datasource_config(ds_name)
            connection_url = config.get_connection_url()
            engine_kwargs = config.get_engine_kwargs()

            logger.info(f"创建数据源 '{ds_name}' 连接: {connection_url}")
            logger.debug(f"连接参数: {engine_kwargs}")

            # 创建引擎
            engine = create_engine(connection_url, **engine_kwargs)

            # 缓存引擎
            self._engines[ds_name] = engine

            return engine

        except Exception as e:
            logger.error(f"创建数据源 '{ds_name}' 连接失败: {str(e)}")
            raise DatabaseError(f"创建数据源连接失败: {str(e)}")

    async def _test_engine_connection(self, engine: Engine, ds_type: str) -> None:
        """测试引擎连接。

        Args:
            engine: SQLAlchemy引擎
            ds_type: 数据源类型

        Raises:
            Exception: 连接测试失败
        """
        try:
            if ds_type.lower() == "hive":
                # Hive使用特殊的测试查询
                logger.debug("正在测试Hive连接...")
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_column"))
                    row = result.fetchone()
                    logger.debug(f"Hive连接测试成功，返回结果: {row}")
            else:
                # 其他数据库使用通用测试
                logger.debug(f"正在测试{ds_type}连接...")
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_column"))
                    row = result.fetchone()
                    logger.debug(f"{ds_type}连接测试成功，返回结果: {row}")
        except ImportError as e:
            if "processors" in str(e) and ds_type.lower() == "hive":
                logger.error(f"Hive驱动兼容性错误: {str(e)}")
                raise Exception(
                    f"Hive连接失败：SQLAlchemy版本与pyhive不兼容。"
                    f"请运行: pip install 'sqlalchemy>=1.4.0,<2.0.0' 来修复此问题。"
                    f"详细错误: {str(e)}"
                )
            elif "sasl" in str(e).lower() and ds_type.lower() == "hive":
                logger.error(f"Hive SASL模块错误: {str(e)}")
                raise Exception(
                    f"Hive连接失败：缺少SASL支持模块。"
                    f"请运行: pip install 'pyhive[hive_pure_sasl]==0.7.0' pure-transport==0.1.0 来修复此问题。"
                    f"详细错误: {str(e)}"
                )
            else:
                raise
        except Exception as e:
            logger.error(f"数据库连接测试失败: {str(e)}")
            # 如果是Hive，提供更详细的错误信息
            if ds_type.lower() == "hive":
                error_msg = f"Hive连接失败: {str(e)}\n"
                error_msg += "请检查:\n"
                error_msg += "1. Hive服务是否正在运行\n"
                error_msg += "2. 端口是否正确（通常是10000）\n"
                error_msg += "3. 认证配置是否正确\n"
                error_msg += "4. SQLAlchemy版本是否与pyhive兼容\n"
                error_msg += "5. 是否正确安装了pure-sasl支持 (对于Python 3.12)"
                raise Exception(error_msg)
            else:
                raise

    async def test_connection(self, ds_name: str) -> bool:
        """测试数据源连接。

        Args:
            ds_name: 数据源名称

        Returns:
            bool: 连接是否成功
        """
        try:
            config = await self.get_datasource_config(ds_name)
            connection_url = config.get_connection_url()
            engine_kwargs = config.get_engine_kwargs()

            logger.info(f"测试数据源 '{ds_name}' 连接")

            # 创建临时引擎进行测试
            test_engine = create_engine(connection_url, **engine_kwargs)

            # 执行连接测试
            await self._test_engine_connection(test_engine, config.ds_type)

            # 清理临时引擎
            test_engine.dispose()

            logger.info(f"数据源 '{ds_name}' 连接测试成功")
            return True

        except Exception as e:
            logger.error(f"数据源 '{ds_name}' 连接测试失败: {str(e)}")
            return False

    async def get_table_partitions(
        self, table_name: str, ds_name: str
    ) -> List[Dict[str, Any]]:
        """获取Hive表的分区信息。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            List[Dict[str, Any]]: 分区信息列表

        Raises:
            DatabaseError: 当获取分区信息失败时
        """
        config = await self.get_datasource_config(ds_name)

        if config.ds_type.lower() != "hive":
            return []  # 非Hive数据源返回空列表

        try:
            engine = await self.get_engine(ds_name)
            with engine.connect() as conn:
                # 获取分区信息
                result = conn.execute(text(f"SHOW PARTITIONS {table_name}"))
                partitions = []

                for row in result:
                    partition_str = row[0] if isinstance(row, tuple) else str(row)
                    # 解析分区字符串，例如 "year=2023/month=12"
                    partition_parts = {}
                    for part in partition_str.split("/"):
                        if "=" in part:
                            key, value = part.split("=", 1)
                            partition_parts[key] = value

                    if partition_parts:
                        partitions.append(partition_parts)

                return partitions

        except Exception as e:
            logger.warning(f"获取表 {table_name} 分区信息失败: {str(e)}")
            return []

    async def get_table_partition_columns(
        self, table_name: str, ds_name: str
    ) -> List[str]:
        """获取Hive表的分区列信息。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            List[str]: 分区列名列表
        """
        config = await self.get_datasource_config(ds_name)

        if config.ds_type.lower() != "hive":
            return []

        try:
            engine = await self.get_engine(ds_name)
            with engine.connect() as conn:
                # 描述表结构
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                partition_columns = []
                in_partition_section = False

                for row in result:
                    row_str = str(row[0]) if isinstance(row, tuple) else str(row)

                    # 检查是否到达分区信息部分
                    if (
                        "# Partition Information" in row_str
                        or "partition_columns" in row_str.lower()
                    ):
                        in_partition_section = True
                        continue

                    # 如果在分区部分，提取分区列
                    if (
                        in_partition_section
                        and row_str.strip()
                        and not row_str.startswith("#")
                    ):
                        col_name = row_str.split()[0]
                        if col_name and col_name != "col_name":
                            partition_columns.append(col_name)

                return partition_columns

        except Exception as e:
            logger.warning(f"获取表 {table_name} 分区列信息失败: {str(e)}")
            return []

    async def is_partitioned_table(self, table_name: str, ds_name: str) -> bool:
        """检查表是否为分区表。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            bool: 是否为分区表
        """
        partition_columns = await self.get_table_partition_columns(table_name, ds_name)
        return len(partition_columns) > 0

    async def list_datasources(self) -> list:
        """列出所有可用的数据源。

        Returns:
            list: 数据源名称列表
        """
        try:
            with self.main_engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT ds_name, ds_type, create_time FROM t_datasource_config ORDER BY create_time"
                    )
                )
                return [
                    {
                        "ds_name": row.ds_name,
                        "ds_type": row.ds_type,
                        "create_time": (
                            row.create_time.isoformat() if row.create_time else None
                        ),
                    }
                    for row in result.fetchall()
                ]
        except SQLAlchemyError as e:
            logger.error(f"获取数据源列表失败: {str(e)}")
            raise DatabaseError(f"获取数据源列表失败: {str(e)}")

    def close_all_connections(self):
        """关闭所有数据源连接。"""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()
        self._configs.clear()
        logger.info("已关闭所有数据源连接")
