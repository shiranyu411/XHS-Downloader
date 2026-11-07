"""High-level MCP tools; CollectionService/Runner own all pagination and downloads."""
from .results import collection_status, export_complete


def register_tools(mcp, service):
    @mcp.tool(description='输入关键词启动完整采集，立即返回目录与进度。后端自动搜索分页、下载帖子与两级评论媒体；用 get_collection_status 查询。')
    async def collect_keyword(keyword: str, cdp: str | None = None) -> dict:
        return await service.collect(keywords=[k.strip() for k in keyword.splitlines() if k.strip()], cdp=cdp)

    @mcp.tool(description='输入完整帖子链接列表，后台完整保存正文、帖子媒体、一级评论、二级回复与评论图片。')
    async def collect_posts(urls: list[str], cdp: str | None = None) -> dict:
        return await service.collect(urls=urls, cdp=cdp)

    @mcp.tool(description='查询采集进度和未完成部分；不向平台发请求。')
    async def get_collection_status(batch: str | None = None) -> dict:
        if batch and str(service.selected) != batch:
            return collection_status(batch)
        return service.status()

    @mcp.tool(description='恢复或补采已有目录，复用已校验文件和有效分页；访问受限后须人工处理，confirmed_access 只用于记录人工确认，不能绕过冷却。')
    async def resume_collection(batch: str | None = None, confirmed_access: bool = False) -> dict:
        return await service.resume_collection(batch, confirmed_access)

    @mcp.tool(description='请求安全暂停后台采集，保存在途响应与恢复进度。')
    async def pause_collection() -> dict:
        service.pause()
        return service.status()

    @mcp.tool(description='导出 posts.csv、comments.csv、comment_media.csv、帖子及评论媒体和 summary.json；不重新请求平台。')
    async def export_collection(batch: str | None = None) -> dict:
        return export_complete(batch) if batch else service.export_collection()
