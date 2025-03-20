import logging
import json
import os
from fastapi import FastAPI, Request, HTTPException
import httpx
from typing import Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="amoCRM Rais Integration")


TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_IDS = {
    "default": "",
}

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


class DataManager:
    """Класс для управления данными из amoCRM"""

    @staticmethod
    def get_status_file_path(scope_id):
        """Возвращает путь к файлу для хранения статусов конкретного аккаунта"""
        return os.path.join(DATA_DIR, f"lead_statuses_{scope_id}.json")

    @staticmethod
    def load_lead_statuses(scope_id):
        """Загружает статусы сделок из JSON файла"""
        file_path = DataManager.get_status_file_path(scope_id)
        try:
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as file:
                    return json.load(file)
            return {}
        except Exception as e:
            logger.error(f"Ошибка загрузки статусов сделок: {str(e)}")
            return {}

    @staticmethod
    def save_lead_statuses(scope_id, statuses):
        """Сохраняет статусы сделок в JSON файл"""
        file_path = DataManager.get_status_file_path(scope_id)
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(statuses, file, ensure_ascii=False, indent=2)
            logger.info(f"Статусы сделок для аккаунта {scope_id} сохранены")
        except Exception as e:
            logger.error(f"Ошибка сохранения статусов сделок: {str(e)}")

    @staticmethod
    def update_lead_status(scope_id, status_id, status_name):
        """Обновляет или добавляет статус сделки"""
        statuses = DataManager.load_lead_statuses(scope_id)
        status_id_str = str(status_id)  # Преобразуем в строку для совместимости с JSON

        if status_id_str not in statuses:
            statuses[status_id_str] = {
                "name": status_name,
                "first_seen": datetime.now().isoformat(),
            }
        else:
            statuses[status_id_str]["name"] = status_name
            statuses[status_id_str]["last_updated"] = datetime.now().isoformat()

        DataManager.save_lead_statuses(scope_id, statuses)
        return statuses

    @staticmethod
    def get_status_name(scope_id, status_id):
        """Возвращает название статуса по его ID"""
        statuses = DataManager.load_lead_statuses(scope_id)
        status_id_str = str(status_id)
        return statuses.get(status_id_str, {}).get("name", f"Статус {status_id}")


class TelegramMessage:
    """Класс для формирования и отправки сообщений в Telegram"""

    @staticmethod
    async def send_message(
        chat_id: str, text: str, parse_mode: str = "HTML"
    ) -> Dict[str, Any]:
        """Отправляет сообщение в Telegram чат"""
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
            )
            return response.json()


class AmoCRMHandler:
    """Класс для обработки данных из amoCRM"""

    @staticmethod
    def format_lead_message(
        lead: Dict[str, Any], event_type: str, scope_id: str
    ) -> str:
        """Форматирует сообщение о сделке"""
        if event_type == "new":
            emoji = "📝"
            title = "Создана новая сделка"
        elif event_type == "update":
            emoji = "🔄"
            title = f"Обновлена сделка"
        elif event_type == "success":
            emoji = "🎉"
            title = "Успешно реализованная сделка"
        else:
            emoji = "ℹ️"
            title = "Информация о сделке"

        message = f"{emoji} <b>{title}</b>\n"
        message += f"Название: {lead.get('name', 'Без названия')}\n"

        if "price" in lead:
            message += f"Бюджет: {lead.get('price', '0')} руб.\n"

        if "responsible_user_name" in lead:
            message += (
                f"Ответственный: {lead.get('responsible_user_name', 'Не назначен')}\n"
            )

        status_id = lead.get("status_id")
        if status_id:
            status_name = None
            if "status_name" in lead:
                status_name = lead["status_name"]

                DataManager.update_lead_status(scope_id, status_id, status_name)

            if not status_name:
                status_name = DataManager.get_status_name(scope_id, status_id)

            message += f"Статус: {status_name}\n"

        return message.strip()

    @staticmethod
    def format_task_message(task: Dict[str, Any], event_type: str) -> str:
        """Форматирует сообщение о задаче"""
        if event_type == "new":
            emoji = "⏰"
            title = "Создана новая задача"
        elif event_type == "completed":
            emoji = "✅"
            title = "Задача выполнена"
        else:
            emoji = "📋"
            title = "Информация о задаче"

        message = f"{emoji} <b>{title}</b>\n"
        message += f"Текст: {task.get('text', 'Без описания')}\n"

        if "complete_till" in task:

            complete_till = task.get("complete_till")
            if isinstance(complete_till, int) and complete_till > 1000000000:
                complete_till = datetime.fromtimestamp(complete_till).strftime(
                    "%d.%m.%Y %H:%M"
                )
            message += f"Срок: {complete_till}\n"

        if "responsible_user_name" in task:
            message += (
                f"Ответственный: {task.get('responsible_user_name', 'Не назначен')}\n"
            )

        return message.strip()

    @staticmethod
    def format_contact_message(contact: Dict[str, Any], event_type: str) -> str:
        """Форматирует сообщение о контакте"""
        if event_type == "new":
            emoji = "👤"
            title = "Создан новый контакт"
        else:
            emoji = "👤"
            title = "Обновлен контакт"

        message = f"{emoji} <b>{title}</b>\n"
        name = contact.get("name", "Без имени")
        message += f"Имя: {name}\n"

        if "custom_fields" in contact:
            for field in contact.get("custom_fields", []):
                if field.get("code") == "PHONE":
                    for value in field.get("values", []):
                        message += f"Телефон: {value.get('value', '')}\n"
                        break
                if field.get("code") == "EMAIL":
                    for value in field.get("values", []):
                        message += f"Email: {value.get('value', '')}\n"
                        break

        return message.strip()


@app.post("/webhooks/amocrm/{scope_id}")
async def amocrm_webhook(scope_id: str, request: Request):
    """Обработчик webhook от amoCRM"""
    try:

        data = await request.json()
        logger.info(f"Получены данные от amoCRM (аккаунт {scope_id}): {data}")

        chat_id = TELEGRAM_CHAT_IDS.get(scope_id, TELEGRAM_CHAT_IDS["default"])

        if "leads" in data:
            await process_leads(data["leads"], chat_id, scope_id)

        if "tasks" in data:
            await process_tasks(data["tasks"], chat_id)

        if "contacts" in data:
            await process_contacts(data["contacts"], chat_id)

        # Обработка данных о статусах, если они есть в данных
        if "lead_statuses" in data:
            process_lead_statuses(data["lead_statuses"], scope_id)

        # Обработка данных о воронках, если они есть
        if "pipelines" in data:
            process_pipelines(data["pipelines"], scope_id)

        return {"status": "success", "message": "Webhook обработан успешно"}

    except Exception as e:
        logger.error(f"Ошибка при обработке webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")


async def process_leads(leads: list, chat_id: str, scope_id: str):
    """Обработка сделок из webhook"""
    for lead in leads:

        if "add" in lead:

            message = AmoCRMHandler.format_lead_message(lead, "new", scope_id)
            await TelegramMessage.send_message(chat_id, message)

        elif lead.get("status_id"):

            successful_status_ids = [
                "142",
                "143",
            ]  # ID статусов успешно реализованных сделок
            if str(lead.get("status_id")) in successful_status_ids:
                message = AmoCRMHandler.format_lead_message(lead, "success", scope_id)
                await TelegramMessage.send_message(chat_id, message)
            else:

                message = AmoCRMHandler.format_lead_message(lead, "update", scope_id)
                await TelegramMessage.send_message(chat_id, message)

        elif "update" in lead:

            message = AmoCRMHandler.format_lead_message(lead, "update", scope_id)
            await TelegramMessage.send_message(chat_id, message)


async def process_tasks(tasks: list, chat_id: str):
    """Обработка задач из webhook"""
    for task in tasks:
        if "add" in task:

            message = AmoCRMHandler.format_task_message(task, "new")
            await TelegramMessage.send_message(chat_id, message)

        elif task.get("is_completed"):

            message = AmoCRMHandler.format_task_message(task, "completed")
            await TelegramMessage.send_message(chat_id, message)

        elif "update" in task:

            message = AmoCRMHandler.format_task_message(task, "update")
            await TelegramMessage.send_message(chat_id, message)


async def process_contacts(contacts: list, chat_id: str):
    """Обработка контактов из webhook"""
    for contact in contacts:
        if "add" in contact:

            message = AmoCRMHandler.format_contact_message(contact, "new")
            await TelegramMessage.send_message(chat_id, message)

        elif "update" in contact:

            message = AmoCRMHandler.format_contact_message(contact, "update")
            await TelegramMessage.send_message(chat_id, message)


def process_lead_statuses(statuses: list, scope_id: str):
    """Обработка информации о статусах сделок"""
    for status in statuses:
        status_id = status.get("id")
        status_name = status.get("name")
        if status_id and status_name:
            DataManager.update_lead_status(scope_id, status_id, status_name)


def process_pipelines(pipelines: list, scope_id: str):
    """Обработка информации о воронках и их статусах"""
    for pipeline in pipelines:
        if "statuses" in pipeline:
            for status in pipeline.get("statuses", []):
                status_id = status.get("id")
                status_name = status.get("name")
                if status_id and status_name:
                    DataManager.update_lead_status(scope_id, status_id, status_name)


@app.get("/status/{scope_id}")
async def get_statuses(scope_id: str):
    """Эндпоинт для просмотра сохраненных статусов"""
    statuses = DataManager.load_lead_statuses(scope_id)
    return {"scope_id": scope_id, "statuses": statuses}
