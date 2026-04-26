from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


DEFAULT_ICONS = {
    "nodes": "✈️",
    "ops": "⚙️",
    "presets": "📦",
    "add": "➕",
    "list": "📁",
    "update": "🔄",
    "ping": "🛜",
    "reboot": "⚠️",
    "edit": "✏️",
    "secret": "❗️",
    "key": "📌",
    "password": "❗️",
    "delete": "🗑",
    "back": "⬅️",
    "home": "🏪",
    "cancel": "❌",
    "confirm": "✅",
    "disabled": "🚫",
    "down": "⬇️",
    "warning": "⚠️",
}
PREMIUM_ICON_IDS = {
    "nodes": "5875465628285931233",
    "ops": "5877260593903177342",
    "delete": "5879896690210639947",
    "edit": "5879841310902324730",
    "confirm": "5776375003280838798",
    "cancel": "5778527486270770928",
    "add": "5775937998948404844",
    "presets": "5924720918826848520",
    "list": "5875206779196935950",
    "update": "5845943483382110702",
    "ping": "5839354140261619193",
    "disabled": "5872829476143894491",
    "secret": "5879813604068298387",
    "warning": "5881702736843511327",
    "key": "5796440171364749940",
    "home": "5983399041197675256",
    "back": "5875082500023258804",
    "down": "5899757765743615694",
    "reboot": "5877410604225924969",
    "password": "5877396173135811032",
}
PREMIUM_FLAG_SETS = (
    "worldroundflags1_by_fStikBot",
    "worldroundflags2_by_fStikBot",
)
ISO_COUNTRY_CODES = {
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW",
    "AX", "AZ", "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN",
    "BO", "BQ", "BR", "BS", "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG",
    "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ",
    "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI",
    "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL",
    "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM", "HN", "HR",
    "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM",
    "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA",
    "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME",
    "MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU",
    "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP",
    "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR",
    "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD",
    "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV",
    "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO",
    "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE",
    "VG", "VI", "VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW",
}
COUNTRY_CODE_ALIASES = {"UK": "GB"}
NO_COUNTRY_FLAG = "🏳️‍🌈"


class KeyboardMixin:
    def _icon(self, key: str, user_id: int | None = None, fallback_icon: str | None = None) -> str:
        if self._can_use_premium_icons():
            return ""
        icon = fallback_icon if fallback_icon is not None else DEFAULT_ICONS.get(key, "")
        return f"{icon} " if icon else ""

    def _button(
        self,
        text: str,
        callback_data: str,
        icon: str | None = None,
        user_id: int | None = None,
        fallback_icon: str | None = None,
        custom_emoji_id: str | None = None,
    ) -> InlineKeyboardButton:
        if self._can_use_premium_icons():
            custom_emoji_id = custom_emoji_id or (PREMIUM_ICON_IDS.get(icon or "") if icon else None)
            if custom_emoji_id:
                try:
                    return InlineKeyboardButton(
                        text=text,
                        callback_data=callback_data,
                        icon_custom_emoji_id=custom_emoji_id,
                    )
                except Exception:
                    self.premium_icons_disabled = True

        return InlineKeyboardButton(
            text=f"{self._icon(icon or '', user_id, fallback_icon=fallback_icon)}{text}",
            callback_data=callback_data,
        )

    def _add_step_keyboard(self, user_id: int, field: str) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        for preset in self.store.list_presets(field):
            token = self._remember_ref(user_id, preset.name)
            rows.append([InlineKeyboardButton(text=preset.name, callback_data=f"add:pre:{field}:{token}")])

        if field == "port":
            rows.insert(0, [InlineKeyboardButton(text="22", callback_data="add:val:port:22")])
        if field == "auth":
            rows.extend(
                [
                    [InlineKeyboardButton(text="SSH-ключ", callback_data="add:auth:key")],
                    [InlineKeyboardButton(text="Пароль", callback_data="add:auth:password")],
                ]
            )
        if field == "ssh_key":
            rows.append([InlineKeyboardButton(text="Назад к способу входа", callback_data="add:back_auth")])
        if field == "password":
            rows.append([InlineKeyboardButton(text="Назад к способу входа", callback_data="add:back_auth")])

        rows.append(self._cancel_row())
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _main_keyboard(self, user_id: int | None = None) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    self._button("Ноды", "nodes:menu", icon="nodes", user_id=user_id),
                    self._button("Операции", "ops:menu", icon="ops", user_id=user_id),
                ],
                [self._button("Пресеты параметров", "presets:menu", icon="presets", user_id=user_id)],
            ]
        )

    def _cancel_keyboard(self, user_id: int | None = None) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[self._cancel_row(user_id)])

    def _home_row(self, user_id: int | None = None) -> list[InlineKeyboardButton]:
        return [self._button("Главное меню", "menu:main", icon="home", user_id=user_id)]

    def _cancel_row(self, user_id: int | None = None) -> list[InlineKeyboardButton]:
        return [self._button("Отмена", "flow:cancel", icon="cancel", user_id=user_id)]

    def _back_home_row(
        self,
        back_callback: str,
        user_id: int | None = None,
    ) -> list[InlineKeyboardButton]:
        return [
            self._button("Назад", back_callback, icon="back", user_id=user_id),
            self._button("Главное меню", "menu:main", icon="home", user_id=user_id),
        ]
