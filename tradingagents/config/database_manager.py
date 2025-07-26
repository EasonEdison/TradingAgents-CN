#!/usr/bin/env python3
"""
智能数据库管理器
自动检测MongoDB和Redis可用性，提供降级方案
使用项目现有的.env配置
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

class DatabaseManager:
    """智能数据库管理器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # 加载.env配置
        self._load_env_config()

        # 数据库连接状态
        self.mongodb_available = False
        self.redis_available = False
        self.mysql_available = False
        self.mongodb_client = None
        self.redis_client = None
        self.mysql_conn = None

        # 检测数据库可用性
        self._detect_databases()

        # 初始化连接
        self._initialize_connections()

        self.logger.info(f"数据库管理器初始化完成 - MongoDB: {self.mongodb_available}, Redis: {self.redis_available}, MySQL: {self.mysql_available}")
    
    def _parse_mysql_url(self, url):
        # 解析mysql://user:pass@host:port/db格式
        import re
        m = re.match(r"mysql://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/([^?]+)", url)
        if not m:
            return None
        return {
            "user": m.group(1),
            "password": m.group(2),
            "host": m.group(3),
            "port": int(m.group(4) or 3306),
            "database": m.group(5)
        }

    def _parse_mongodb_url(self, url):
        # 解析mongodb://user:pass@host:port/db?authSource=xxx格式
        import re
        m = re.match(r"mongodb://([^:]+):([^@]+)@([^:/]+)(?::(\d+))?/([^?]+)(?:\?authSource=([^&]+))?", url)
        if not m:
            return None
        return {
            "username": m.group(1),
            "password": m.group(2),
            "host": m.group(3),
            "port": int(m.group(4) or 27017),
            "database": m.group(5),
            "auth_source": m.group(6) or "admin"
        }

    def _parse_redis_url(self, url):
        # 解析redis://:password@host:port/db格式
        import re
        m = re.match(r"redis://:(.*?)@([^:/]+)(?::(\d+))?", url)
        if not m:
            return None
        return {
            "password": m.group(1),
            "host": m.group(2),
            "port": int(m.group(3) or 6379)
        }

    def _load_env_config(self):
        """从.env文件加载配置"""
        # 尝试加载python-dotenv
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            self.logger.info("python-dotenv未安装，直接读取环境变量")

        # 读取启用开关
        self.mongodb_enabled = os.getenv("MONGODB_ENABLED", "false").lower() == "true"
        self.redis_enabled = os.getenv("REDIS_ENABLED", "false").lower() == "true"

        # 从环境变量读取MongoDB配置
        self.mongodb_config = {
            "enabled": self.mongodb_enabled,
            "host": os.getenv("MONGODB_HOST", "localhost"),
            "port": int(os.getenv("MONGODB_PORT", "27017")),
            "username": os.getenv("MONGODB_USERNAME"),
            "password": os.getenv("MONGODB_PASSWORD"),
            "database": os.getenv("MONGODB_DATABASE", "tradingagents"),
            "auth_source": os.getenv("MONGODB_AUTH_SOURCE", "admin"),
            "timeout": 2000
        }

        # 从环境变量读取Redis配置
        self.redis_config = {
            "enabled": self.redis_enabled,
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "password": os.getenv("REDIS_PASSWORD"),
            "db": int(os.getenv("REDIS_DB", "0")),
            "timeout": 2
        }

        # MongoDB配置优先级：TRADINGAGENTS_MONGODB_URL > 单独变量 > Docker智能切换 > 默认
        mongodb_url = os.getenv("TRADINGAGENTS_MONGODB_URL")
        if mongodb_url:
            parsed = self._parse_mongodb_url(mongodb_url)
            if parsed:
                self.mongodb_config = {"enabled": True, **parsed, "timeout": 2000}
            else:
                self.mongodb_config = {"enabled": True, "host": "mongodb", "port": 27017, "username": "admin", "password": "tradingagents123", "database": "tradingagents", "auth_source": "admin", "timeout": 2000}
        else:
            def smart_mongodb_host():
                if os.getenv("MONGODB_HOST"):
                    return os.getenv("MONGODB_HOST")
                elif os.getenv("DOCKER_CONTAINER", "").lower() == "true":
                    return "mongodb"
                else:
                    return "localhost"
            self.mongodb_config = {
                "enabled": os.getenv("MONGODB_ENABLED", "true").lower() == "true",
                "host": smart_mongodb_host(),
                "port": int(os.getenv("MONGODB_PORT", "27017")),
                "username": os.getenv("MONGODB_USERNAME", "admin"),
                "password": os.getenv("MONGODB_PASSWORD", "tradingagents123"),
                "database": os.getenv("MONGODB_DATABASE", "tradingagents"),
                "auth_source": os.getenv("MONGODB_AUTH_SOURCE", "admin"),
                "timeout": 2000
            }
        self.logger.info(f"MongoDB启用: {self.mongodb_config['enabled']}")
        if self.mongodb_config["enabled"]:
            self.logger.info(f"MongoDB配置: {self.mongodb_config['host']}:{self.mongodb_config['port']}")

        # Redis配置优先级：TRADINGAGENTS_REDIS_URL > 单独变量 > Docker智能切换 > 默认
        redis_url = os.getenv("TRADINGAGENTS_REDIS_URL")
        if redis_url:
            parsed = self._parse_redis_url(redis_url)
            if parsed:
                self.redis_config = {"enabled": True, **parsed, "db": int(os.getenv("REDIS_DB", "0")), "timeout": 2}
            else:
                self.redis_config = {"enabled": True, "host": "redis", "port": 6379, "password": "tradingagents123", "db": 0, "timeout": 2}
        else:
            def smart_redis_host():
                if os.getenv("REDIS_HOST"):
                    return os.getenv("REDIS_HOST")
                elif os.getenv("DOCKER_CONTAINER", "").lower() == "true":
                    return "redis"
                else:
                    return "localhost"
            self.redis_config = {
                "enabled": os.getenv("REDIS_ENABLED", "true").lower() == "true",
                "host": smart_redis_host(),
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "password": os.getenv("REDIS_PASSWORD", "tradingagents123"),
                "db": int(os.getenv("REDIS_DB", "0")),
                "timeout": 2
            }
        self.logger.info(f"Redis启用: {self.redis_config['enabled']}")
        if self.redis_config["enabled"]:
            self.logger.info(f"Redis配置: {self.redis_config['host']}:{self.redis_config['port']}")

        # MySQL配置优先级：TRADINGAGENTS_MYSQL_URL > 单独变量 > Docker智能切换 > 默认
        mysql_url = os.getenv("TRADINGAGENTS_MYSQL_URL")
        if mysql_url:
            parsed = self._parse_mysql_url(mysql_url)
            if parsed:
                self.mysql_config = {"enabled": True, **parsed}
            else:
                self.mysql_config = {"enabled": True, "host": "mysql", "port": 3306, "user": "tradinguser", "password": "tradinguser123", "database": "tradingagents"}
        else:
            def smart_mysql_host():
                if os.getenv("MYSQL_HOST"):
                    return os.getenv("MYSQL_HOST")
                elif os.getenv("DOCKER_CONTAINER", "").lower() == "true":
                    return "mysql"
                else:
                    return "localhost"
            self.mysql_config = {
                "enabled": os.getenv("MYSQL_ENABLED", "true").lower() == "true",
                "host": smart_mysql_host(),
                "port": int(os.getenv("MYSQL_PORT", "3306")),
                "user": os.getenv("MYSQL_USER", "tradinguser"),
                "password": os.getenv("MYSQL_PASSWORD", "tradinguser123"),
                "database": os.getenv("MYSQL_DATABASE", "tradingagents"),
            }
        self.logger.info(f"MySQL启用: {self.mysql_config['enabled']}")
        if self.mysql_config["enabled"]:
            self.logger.info(f"MySQL配置: {self.mysql_config['host']}:{self.mysql_config['port']}")

        self.logger.info(f"MongoDB启用: {self.mongodb_enabled}")
        self.logger.info(f"Redis启用: {self.redis_enabled}")
        if self.mongodb_enabled:
            self.logger.info(f"MongoDB配置: {self.mongodb_config['host']}:{self.mongodb_config['port']}")
        if self.redis_enabled:
            self.logger.info(f"Redis配置: {self.redis_config['host']}:{self.redis_config['port']}")
    

    
    def _detect_mongodb(self) -> Tuple[bool, str]:
        """检测MongoDB是否可用"""
        # 首先检查是否启用
        if not self.mongodb_enabled:
            return False, "MongoDB未启用 (MONGODB_ENABLED=false)"

        try:
            import pymongo
            from pymongo import MongoClient

            # 构建连接参数
            connect_kwargs = {
                "host": self.mongodb_config["host"],
                "port": self.mongodb_config["port"],
                "serverSelectionTimeoutMS": self.mongodb_config["timeout"],
                "connectTimeoutMS": self.mongodb_config["timeout"]
            }

            # 如果有用户名和密码，添加认证
            if self.mongodb_config["username"] and self.mongodb_config["password"]:
                connect_kwargs.update({
                    "username": self.mongodb_config["username"],
                    "password": self.mongodb_config["password"],
                    "authSource": self.mongodb_config["auth_source"]
                })

            client = MongoClient(**connect_kwargs)

            # 测试连接
            client.server_info()
            client.close()

            return True, "MongoDB连接成功"

        except ImportError:
            return False, "pymongo未安装"
        except Exception as e:
            return False, f"MongoDB连接失败: {str(e)}"
    
    def _detect_redis(self) -> Tuple[bool, str]:
        """检测Redis是否可用"""
        # 首先检查是否启用
        if not self.redis_enabled:
            return False, "Redis未启用 (REDIS_ENABLED=false)"

        try:
            import redis

            # 构建连接参数
            connect_kwargs = {
                "host": self.redis_config["host"],
                "port": self.redis_config["port"],
                "db": self.redis_config["db"],
                "socket_timeout": self.redis_config["timeout"],
                "socket_connect_timeout": self.redis_config["timeout"]
            }

            # 如果有密码，添加密码
            if self.redis_config["password"]:
                connect_kwargs["password"] = self.redis_config["password"]

            client = redis.Redis(**connect_kwargs)

            # 测试连接
            client.ping()

            return True, "Redis连接成功"

        except ImportError:
            return False, "redis未安装"
        except Exception as e:
            return False, f"Redis连接失败: {str(e)}"
    
    def _detect_databases(self):
        """检测所有数据库"""
        self.logger.info("开始检测数据库可用性...")
        
        # 检测MongoDB
        mongodb_available, mongodb_msg = self._detect_mongodb()
        self.mongodb_available = mongodb_available
        
        if mongodb_available:
            self.logger.info(f"✅ MongoDB: {mongodb_msg}")
        else:
            self.logger.info(f"❌ MongoDB: {mongodb_msg}")
        
        # 检测Redis
        redis_available, redis_msg = self._detect_redis()
        self.redis_available = redis_available
        
        if redis_available:
            self.logger.info(f"✅ Redis: {redis_msg}")
        else:
            self.logger.info(f"❌ Redis: {redis_msg}")
        
        # 更新配置
        self._update_config_based_on_detection()
    
    def _update_config_based_on_detection(self):
        """根据检测结果更新配置"""
        # 确定缓存后端
        if self.redis_available:
            self.primary_backend = "redis"
        elif self.mongodb_available:
            self.primary_backend = "mongodb"
        else:
            self.primary_backend = "file"

        self.logger.info(f"主要缓存后端: {self.primary_backend}")
    
    def _initialize_connections(self):
        """初始化数据库连接"""
        # 初始化MongoDB连接
        if self.mongodb_available:
            try:
                import pymongo

                # 构建连接参数
                connect_kwargs = {
                    "host": self.mongodb_config["host"],
                    "port": self.mongodb_config["port"],
                    "serverSelectionTimeoutMS": self.mongodb_config["timeout"]
                }

                # 如果有用户名和密码，添加认证
                if self.mongodb_config["username"] and self.mongodb_config["password"]:
                    connect_kwargs.update({
                        "username": self.mongodb_config["username"],
                        "password": self.mongodb_config["password"],
                        "authSource": self.mongodb_config["auth_source"]
                    })

                self.mongodb_client = pymongo.MongoClient(**connect_kwargs)
                self.logger.info("MongoDB客户端初始化成功")
            except Exception as e:
                self.logger.error(f"MongoDB客户端初始化失败: {e}")
                self.mongodb_available = False

        # 初始化Redis连接
        if self.redis_available:
            try:
                import redis

                # 构建连接参数
                connect_kwargs = {
                    "host": self.redis_config["host"],
                    "port": self.redis_config["port"],
                    "db": self.redis_config["db"],
                    "socket_timeout": self.redis_config["timeout"]
                }

                # 如果有密码，添加密码
                if self.redis_config["password"]:
                    connect_kwargs["password"] = self.redis_config["password"]

                self.redis_client = redis.Redis(**connect_kwargs)
                self.logger.info("Redis客户端初始化成功")
            except Exception as e:
                self.logger.error(f"Redis客户端初始化失败: {e}")
                self.redis_available = False

        # 初始化MySQL连接
        if self.mysql_config["enabled"]:
            try:
                import pymysql
                self.mysql_conn = pymysql.connect(
                    host=self.mysql_config["host"],
                    port=self.mysql_config["port"],
                    user=self.mysql_config["user"],
                    password=self.mysql_config["password"],
                    database=self.mysql_config["database"],
                    charset="utf8mb4",
                    autocommit=True
                )
                self.mysql_available = True
                self.logger.info("MySQL客户端初始化成功")
            except Exception as e:
                self.logger.error(f"MySQL客户端初始化失败: {e}")
                self.mysql_available = False
    
    def get_mongodb_client(self):
        """获取MongoDB客户端"""
        if self.mongodb_available and self.mongodb_client:
            return self.mongodb_client
        return None
    
    def get_redis_client(self):
        """获取Redis客户端"""
        if self.redis_available and self.redis_client:
            return self.redis_client
        return None
    
    def is_mongodb_available(self) -> bool:
        """检查MongoDB是否可用"""
        return self.mongodb_available
    
    def is_redis_available(self) -> bool:
        """检查Redis是否可用"""
        return self.redis_available
    
    def is_database_available(self) -> bool:
        """检查是否有任何数据库可用"""
        return self.mongodb_available or self.redis_available
    
    def get_cache_backend(self) -> str:
        """获取当前缓存后端"""
        return self.primary_backend

    def get_config(self) -> Dict[str, Any]:
        """获取配置信息"""
        return {
            "mongodb": self.mongodb_config,
            "redis": self.redis_config,
            "primary_backend": self.primary_backend,
            "mongodb_available": self.mongodb_available,
            "redis_available": self.redis_available
        }

    def get_status_report(self) -> Dict[str, Any]:
        """获取状态报告"""
        return {
            "database_available": self.is_database_available(),
            "mongodb": {
                "available": self.mongodb_available,
                "host": self.mongodb_config["host"],
                "port": self.mongodb_config["port"]
            },
            "redis": {
                "available": self.redis_available,
                "host": self.redis_config["host"],
                "port": self.redis_config["port"]
            },
            "cache_backend": self.get_cache_backend(),
            "fallback_enabled": True  # 总是启用降级
        }

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        stats = {
            "mongodb_available": self.mongodb_available,
            "redis_available": self.redis_available,
            "redis_keys": 0,
            "redis_memory": "N/A"
        }

        # Redis统计
        if self.redis_available and self.redis_client:
            try:
                info = self.redis_client.info()
                stats["redis_keys"] = self.redis_client.dbsize()
                stats["redis_memory"] = info.get("used_memory_human", "N/A")
            except Exception as e:
                self.logger.error(f"获取Redis统计失败: {e}")

        return stats

    def cache_clear_pattern(self, pattern: str) -> int:
        """清理匹配模式的缓存"""
        cleared_count = 0

        if self.redis_available and self.redis_client:
            try:
                keys = self.redis_client.keys(pattern)
                if keys:
                    cleared_count += self.redis_client.delete(*keys)
            except Exception as e:
                self.logger.error(f"Redis缓存清理失败: {e}")

        return cleared_count

    def get_mysql_conn(self):
        """获取MySQL连接"""
        if self.mysql_available and self.mysql_conn:
            return self.mysql_conn
        return None

    def is_mysql_available(self) -> bool:
        """检查MySQL是否可用"""
        return self.mysql_available

    def create_tables_if_not_exist(self):
        """自动建表：report_sessions和analysis_reports"""
        if not self.is_mysql_available():
            return
        conn = self.get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_symbol VARCHAR(32) NOT NULL,
                market_type VARCHAR(8) NOT NULL,
                analysis_date DATE NOT NULL,
                final_advice VARCHAR(64) DEFAULT NULL,
                decision_summary TEXT DEFAULT NULL,
                decision_action VARCHAR(64) DEFAULT NULL,
                decision_confidence FLOAT DEFAULT NULL,
                decision_risk_score FLOAT DEFAULT NULL,
                decision_target_price VARCHAR(64) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analysis_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT NOT NULL,
                report_type VARCHAR(32) NOT NULL,
                report_markdown LONGTEXT NOT NULL,
                advice VARCHAR(64) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES report_sessions(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        conn.commit()
        cursor.close()

    def insert_report_session(self, stock_symbol, market_type, analysis_date, final_advice=None, decision_summary=None, decision_action=None, decision_confidence=None, decision_risk_score=None, decision_target_price=None):
        """插入一条report_sessions，返回session_id"""
        if not self.is_mysql_available():
            return None
        conn = self.get_mysql_conn()
        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO report_sessions (stock_symbol, market_type, analysis_date, final_advice, decision_summary, decision_action, decision_confidence, decision_risk_score, decision_target_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (stock_symbol, market_type, analysis_date, final_advice, decision_summary, decision_action, decision_confidence, decision_risk_score, decision_target_price))
        conn.commit()
        session_id = cursor.lastrowid
        cursor.close()
        return session_id

    def insert_analysis_report(self, session_id, report_type, report_markdown, advice=None):
        """插入一条analysis_reports"""
        if not self.is_mysql_available():
            return
        conn = self.get_mysql_conn()
        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO analysis_reports (session_id, report_type, report_markdown, advice)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (session_id, report_type, report_markdown, advice))
        conn.commit()
        cursor.close()


# 全局数据库管理器实例
_database_manager = None

def get_database_manager() -> DatabaseManager:
    """获取全局数据库管理器实例"""
    global _database_manager
    if _database_manager is None:
        _database_manager = DatabaseManager()
    return _database_manager

def is_mongodb_available() -> bool:
    """检查MongoDB是否可用"""
    return get_database_manager().is_mongodb_available()

def is_redis_available() -> bool:
    """检查Redis是否可用"""
    return get_database_manager().is_redis_available()

def get_cache_backend() -> str:
    """获取当前缓存后端"""
    return get_database_manager().get_cache_backend()

def get_mongodb_client():
    """获取MongoDB客户端"""
    return get_database_manager().get_mongodb_client()

def get_redis_client():
    """获取Redis客户端"""
    return get_database_manager().get_redis_client()
