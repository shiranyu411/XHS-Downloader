from typing import TYPE_CHECKING

from curl_cffi.requests import get
from curl_cffi.requests.exceptions import RequestException

from ..module.access import AccessRestricted, refusal_reason
from ..module import ERROR, logging, retry, sleep_time
from ..translation import _

if TYPE_CHECKING:
    from ..module import Manager

__all__ = ["Html"]


class Html:
    def __init__(
        self,
        manager: "Manager",
    ):
        self.manager = manager
        self.print = manager.print
        self.retry = manager.retry
        self.client = manager.request_client
        self.timeout = manager.timeout
        self.proxy = manager.proxy
        self.impersonate = manager.impersonate
        self.access_restriction = None
        self.on_restriction = None

    @retry
    async def request_url(
        self,
        url: str,
        content=True,
        cookie: str | None = None,
        proxy: str | None = None,
        **kwargs,
    ) -> str:
        if self.access_restriction:
            raise AccessRestricted(self.access_restriction)
        if not url.startswith("http"):
            url = f"https://{url}"
        headers = self.manager.get_headers(url)
        try:
            if cookie:
                response = self.__request_url_with_cookie(
                    url,
                    headers,
                    cookie,
                    proxy=proxy,
                    **kwargs,
                )
            else:
                response = await self.__request_url_get(
                    url,
                    headers,
                    proxy=proxy,
                    **kwargs,
                )
            reason = refusal_reason(response.status_code, response.text, response.url)
            if reason:
                self.access_restriction = reason
                if self.on_restriction:
                    self.on_restriction(reason)
                # This exception is deliberately not a RequestException: @retry must stop.
                raise AccessRestricted(reason)
            await sleep_time(self.manager.avg_delay)
            response.raise_for_status()
            return response.text if content else str(response.url)
        except RequestException as error:
            logging(
                self.print,
                _("网络异常，{0} 请求失败: {1}").format(url, repr(error)),
                ERROR,
            )
            return ""

    @staticmethod
    def format_url(url: str) -> str:
        return bytes(url, "utf-8").decode("unicode_escape")

    def __request_url_with_cookie(
        self,
        url: str,
        headers: dict,
        cookie: str,
        proxy: str | None = None,
        **kwargs,
    ):
        return get(
            url,
            headers=headers,
            cookies=self.manager.cookie_str_to_cookies(cookie),
            timeout=self.timeout,
            verify=False,
            allow_redirects=True,
            proxy=self.proxy if proxy is None else proxy,
            impersonate=self.impersonate,
            **kwargs,
        )

    async def __request_url_get(
        self,
        url: str,
        headers: dict,
        proxy: str | None = None,
        **kwargs,
    ):
        return await self.client.get(
            url,
            headers=headers,
            proxy=self.proxy if proxy is None else proxy,
            **kwargs,
        )
