"""主应用入口文件。"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.datasource_config import router as datasource_config_router
from app.api.sql_service import router as sql_router
from app.api.user import router as user_router

app = FastAPI(
    title="OpenManus API", description="OpenManus SQL 服务 API", version="1.0.0"
)

# 配置 CORS - 支持跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有域名，生产环境建议限制具体域名
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "Accept",
        "Origin",
        "X-Requested-With",
        "X-User-ID",
        "Cache-Control",
        "Access-Control-Request-Method",
        "Access-Control-Request-Headers",
    ],
    expose_headers=["Content-Length", "Content-Range"],
    max_age=3600,
)

# 注册路由
app.include_router(sql_router)
app.include_router(datasource_config_router)
app.include_router(user_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
