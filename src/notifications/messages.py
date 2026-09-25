"""The account emails (AUTH-RECOVERY), in the UI's languages: Russian, English, Spanish.

The language comes from the request's Accept-Language header (preferred_language), with
English as the fallback. Every message is plain text and names the account's address, so
a recipient can tell which account it is about.
"""
from __future__ import annotations

from enum import Enum

from src.notifications.mail import OutgoingEmail

APP_NAME = "Veris"
SUPPORTED_LANGUAGES = ("en", "ru", "es")
DEFAULT_LANGUAGE = "en"


class AccountEmail(str, Enum):
    PASSWORD_RESET = "password_reset"  # carries a reset link
    EMAIL_VERIFICATION = "email_verification"  # carries a verification link
    PASSWORD_RESET_DONE = "password_reset_done"
    GOOGLE_LINKED = "google_linked"  # the local password was kept
    GOOGLE_LINKED_PASSWORD_REMOVED = "google_linked_password_removed"  # it was not verified
    PASSWORD_CHANGED = "password_changed"  # in Settings


def preferred_language(accept_language: str | None) -> str:
    """The supported language the Accept-Language header ranks highest ("ru-RU,ru;q=0.9,
    en;q=0.8" -> "ru"), else English. Malformed entries are skipped."""
    ranked: list[tuple[float, int, str]] = []
    for position, entry in enumerate((accept_language or "").split(",")[:32]):
        tag, _, params = entry.strip().partition(";")
        primary = tag.strip().split("-")[0].lower()
        quality = 1.0
        params = params.strip()
        if params.startswith("q="):
            try:
                quality = float(params[2:])
            except ValueError:
                continue
        if primary in SUPPORTED_LANGUAGES and quality > 0:
            ranked.append((-quality, position, primary))
    return min(ranked)[2] if ranked else DEFAULT_LANGUAGE


def _plural_ru(count: int, one: str, few: str, many: str) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return one
    if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14:
        return few
    return many


def _duration(seconds: int, language: str) -> str:
    """How long a link works, in whole hours when it is a whole number of them (at least
    two), else in minutes: "60 minutes", "24 hours"."""
    seconds = max(int(seconds), 60)
    if seconds % 3600 == 0 and seconds >= 7200:
        count, unit = seconds // 3600, "hour"
    else:
        count, unit = max(seconds // 60, 1), "minute"
    if language == "ru":
        forms = ("час", "часа", "часов") if unit == "hour" else ("минуту", "минуты", "минут")
        return f"{count} {_plural_ru(count, *forms)}"
    if language == "es":
        word = ("hora" if unit == "hour" else "minuto") + ("" if count == 1 else "s")
        return f"{count} {word}"
    return f"{count} {unit}{'' if count == 1 else 's'}"


_FOOTER = {
    "en": f"— {APP_NAME}\nThis message was sent automatically; replies are not read.",
    "ru": f"— {APP_NAME}\nЭто письмо отправлено автоматически, ответы на него не читаются.",
    "es": f"— {APP_NAME}\nEste correo se ha enviado automáticamente; las respuestas no se leen.",
}

# (subject, body) per message and language. Placeholders: {email}, {link}, {valid_for},
# {app_url}.
_TEMPLATES: dict[AccountEmail, dict[str, tuple[str, str]]] = {
    AccountEmail.PASSWORD_RESET: {
        "en": (
            f"Reset your {APP_NAME} password",
            f"Hello,\n\nWe received a request to reset the password of the {APP_NAME} account {{email}}.\n\n"
            "To choose a new password, open this link (it works for {valid_for}):\n\n{link}\n\n"
            "The link works once. Resetting the password signs the account out on every device.\n\n"
            "If you did not ask for this, ignore this email: your password stays as it is.",
        ),
        "ru": (
            f"Сброс пароля {APP_NAME}",
            f"Здравствуйте!\n\nМы получили запрос на сброс пароля аккаунта {APP_NAME} {{email}}.\n\n"
            "Чтобы задать новый пароль, откройте ссылку (она действует {valid_for}):\n\n{link}\n\n"
            "Ссылкой можно воспользоваться один раз. После сброса пароля аккаунт выйдет из системы "
            "на всех устройствах.\n\n"
            "Если вы не запрашивали сброс, проигнорируйте это письмо: пароль останется прежним.",
        ),
        "es": (
            f"Restablece tu contraseña de {APP_NAME}",
            f"Hola:\n\nHemos recibido una solicitud para restablecer la contraseña de la cuenta de {APP_NAME} "
            "{email}.\n\n"
            "Para elegir una contraseña nueva, abre este enlace (es válido durante {valid_for}):\n\n{link}\n\n"
            "El enlace solo se puede usar una vez. Al restablecer la contraseña, se cerrará la sesión "
            "de la cuenta en todos los dispositivos.\n\n"
            "Si no lo has solicitado, ignora este correo: tu contraseña no cambiará.",
        ),
    },
    AccountEmail.EMAIL_VERIFICATION: {
        "en": (
            f"Confirm your email address for {APP_NAME}",
            f"Hello,\n\nTo confirm that {{email}} is your address for {APP_NAME}, open this link "
            "(it works for {valid_for}):\n\n{link}\n\n"
            f"If you did not create a {APP_NAME} account with this address, do not open the link: "
            "someone else may have entered your address. You can ignore this email.",
        ),
        "ru": (
            f"Подтвердите адрес почты для {APP_NAME}",
            f"Здравствуйте!\n\nЧтобы подтвердить, что {{email}} — ваш адрес для {APP_NAME}, откройте "
            "ссылку (она действует {valid_for}):\n\n{link}\n\n"
            f"Если вы не создавали аккаунт {APP_NAME} с этим адресом, не открывайте ссылку: возможно, "
            "ваш адрес указал кто-то другой. Письмо можно проигнорировать.",
        ),
        "es": (
            f"Confirma tu dirección de correo para {APP_NAME}",
            f"Hola:\n\nPara confirmar que {{email}} es tu dirección para {APP_NAME}, abre este enlace "
            "(es válido durante {valid_for}):\n\n{link}\n\n"
            f"Si no has creado una cuenta de {APP_NAME} con esta dirección, no abras el enlace: puede "
            "que otra persona haya introducido tu dirección. Puedes ignorar este correo.",
        ),
    },
    AccountEmail.PASSWORD_RESET_DONE: {
        "en": (
            f"Your {APP_NAME} password was reset",
            f"Hello,\n\nThe password of the {APP_NAME} account {{email}} was just reset through a link "
            "sent to this address, and the account was signed out on every device.\n\n"
            "If this was not you, someone can read this mailbox: secure it, then reset the password "
            "again at {app_url}/forgot-password",
        ),
        "ru": (
            f"Пароль {APP_NAME} сброшен",
            f"Здравствуйте!\n\nПароль аккаунта {APP_NAME} {{email}} только что сброшен по ссылке, "
            "отправленной на этот адрес, и аккаунт вышел из системы на всех устройствах.\n\n"
            "Если это были не вы, значит, у кого-то есть доступ к этому почтовому ящику: защитите его, "
            "а затем снова сбросьте пароль: {app_url}/forgot-password",
        ),
        "es": (
            f"Se ha restablecido tu contraseña de {APP_NAME}",
            f"Hola:\n\nLa contraseña de la cuenta de {APP_NAME} {{email}} se acaba de restablecer mediante "
            "un enlace enviado a esta dirección, y se ha cerrado la sesión en todos los dispositivos.\n\n"
            "Si no has sido tú, alguien tiene acceso a este buzón: protégelo y, después, vuelve a "
            "restablecer la contraseña: {app_url}/forgot-password",
        ),
    },
    AccountEmail.GOOGLE_LINKED: {
        "en": (
            f"Google sign-in was linked to your {APP_NAME} account",
            f"Hello,\n\nGoogle sign-in was linked to the {APP_NAME} account {{email}}. You can now sign "
            "in with Google as well as with your password.\n\n"
            "If this was not you, sign in, change the password and sign out on all devices in "
            "Settings: {app_url}/settings",
        ),
        "ru": (
            f"К аккаунту {APP_NAME} привязан вход через Google",
            f"Здравствуйте!\n\nК аккаунту {APP_NAME} {{email}} привязан вход через Google. Теперь можно "
            "входить и через Google, и по паролю.\n\n"
            "Если это были не вы, войдите, смените пароль и выйдите на всех устройствах в настройках: "
            "{app_url}/settings",
        ),
        "es": (
            f"Se ha vinculado el inicio de sesión con Google a tu cuenta de {APP_NAME}",
            f"Hola:\n\nSe ha vinculado el inicio de sesión con Google a la cuenta de {APP_NAME} {{email}}. "
            "Ahora puedes iniciar sesión con Google o con tu contraseña.\n\n"
            "Si no has sido tú, inicia sesión, cambia la contraseña y cierra la sesión en todos los "
            "dispositivos en los ajustes: {app_url}/settings",
        ),
    },
    AccountEmail.GOOGLE_LINKED_PASSWORD_REMOVED: {
        "en": (
            f"Google sign-in was linked to your {APP_NAME} account",
            f"Hello,\n\nGoogle sign-in was linked to the {APP_NAME} account {{email}}. The address had "
            "not been confirmed, so the password the account was registered with was removed and "
            "every device was signed out: whoever registered it can no longer sign in.\n\n"
            "Sign in with Google from now on. You can set a new password in Settings: "
            "{app_url}/settings",
        ),
        "ru": (
            f"К аккаунту {APP_NAME} привязан вход через Google",
            f"Здравствуйте!\n\nК аккаунту {APP_NAME} {{email}} привязан вход через Google. Адрес не был "
            "подтверждён, поэтому пароль, с которым регистрировался аккаунт, удалён, а все устройства "
            "вышли из системы: тот, кто зарегистрировал аккаунт, больше не сможет войти.\n\n"
            "Теперь входите через Google. Новый пароль можно задать в настройках: {app_url}/settings",
        ),
        "es": (
            f"Se ha vinculado el inicio de sesión con Google a tu cuenta de {APP_NAME}",
            f"Hola:\n\nSe ha vinculado el inicio de sesión con Google a la cuenta de {APP_NAME} {{email}}. "
            "La dirección no estaba confirmada, así que se ha eliminado la contraseña con la que se "
            "registró la cuenta y se ha cerrado la sesión en todos los dispositivos: quien la "
            "registró ya no puede iniciar sesión.\n\n"
            "A partir de ahora, inicia sesión con Google. Puedes establecer una contraseña nueva en "
            "los ajustes: {app_url}/settings",
        ),
    },
    AccountEmail.PASSWORD_CHANGED: {
        "en": (
            f"Your {APP_NAME} password was changed",
            f"Hello,\n\nA new password was set for the {APP_NAME} account {{email}} from a signed-in "
            "session, and every other device was signed out.\n\n"
            "If this was not you, reset the password: {app_url}/forgot-password",
        ),
        "ru": (
            f"Пароль {APP_NAME} изменён",
            f"Здравствуйте!\n\nДля аккаунта {APP_NAME} {{email}} из открытого сеанса задан новый пароль, "
            "а все остальные устройства вышли из системы.\n\n"
            "Если это были не вы, сбросьте пароль: {app_url}/forgot-password",
        ),
        "es": (
            f"Se ha cambiado tu contraseña de {APP_NAME}",
            f"Hola:\n\nSe ha establecido una contraseña nueva para la cuenta de {APP_NAME} {{email}} desde "
            "una sesión iniciada, y se ha cerrado la sesión en todos los demás dispositivos.\n\n"
            "Si no has sido tú, restablece la contraseña: {app_url}/forgot-password",
        ),
    },
}


def render_account_email(
    kind: AccountEmail,
    language: str,
    *,
    to: str,
    app_url: str,
    link: str = "",
    valid_for_seconds: int = 0,
) -> OutgoingEmail:
    """The message ``kind`` for ``to``, in ``language`` (English when unsupported)."""
    language = language if language in SUPPORTED_LANGUAGES else DEFAULT_LANGUAGE
    subject, body = _TEMPLATES[AccountEmail(kind)][language]
    text = body.format(
        email=to,
        link=link,
        valid_for=_duration(valid_for_seconds, language) if valid_for_seconds else "",
        app_url=app_url.rstrip("/"),
    )
    return OutgoingEmail(to=to, subject=subject, body=f"{text}\n\n{_FOOTER[language]}\n")
