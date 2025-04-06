"""主应用入口文件。"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.sql_service import router as sql_router

app = FastAPI(
    title="OpenManus API",
    description="OpenManus SQL 服务 API",
    version="1.0.0"
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(sql_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
