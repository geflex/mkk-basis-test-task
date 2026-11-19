"""Проверка webhook-адресов (защита от SSRF)."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

_LOCAL_HOSTNAMES = ("localhost",)
_LOCAL_SUFFIXES = (".localhost", ".local", ".internal")


class UnsafeWebhookTarget(ValueError):
    """Webhook-адрес указывает на недопустимый хост."""


def is_private_host(host: str) -> bool:
    """Возвращает ``True`` для хостов, недоступных внешнему получателю."""

    host = host.strip().lower().rstrip(".")
    if host in _LOCAL_HOSTNAMES or host.endswith(_LOCAL_SUFFIXES):
        return True
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
        or address.is_unspecified
    )


async def validate_webhook_target(url: str, allow_private_hosts: bool = False) -> None:
    """Проверяет URL перед отправкой, включая DNS-резолвинг.

    Валидация на этапе создания платежа проверяет только литерал хоста.
    Повторная проверка перед отправкой закрывает DNS rebinding и CNAME на
    внутренние адреса.
    """

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise UnsafeWebhookTarget(f"неподдерживаемая схема webhook: {parsed.scheme!r}")

    host = parsed.hostname
    if not host:
        raise UnsafeWebhookTarget("webhook_url не содержит хост")

    if allow_private_hosts:
        return

    if is_private_host(host):
        raise UnsafeWebhookTarget(
            "webhook_url не должен указывать на приватный, loopback- "
            "или link-local хост"
        )

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise UnsafeWebhookTarget(f"не удалось разрешить хост {host!r}") from exc

    for info in infos:
        address = info[4][0]
        if is_private_host(address):
            raise UnsafeWebhookTarget(
                f"webhook_url {host!r} разрешается в недопустимый адрес {address!r}"
            )