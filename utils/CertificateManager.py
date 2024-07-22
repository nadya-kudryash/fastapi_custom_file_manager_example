import base64
import io
import urllib
from dataclasses import dataclass
from typing import List, Dict, Tuple, Union

import hashlib
import os
import pathlib
import string
from datetime import datetime
from random import choice
from uuid import uuid4, UUID

import aiofiles
from fastapi import Depends, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeMeta

from database import get_session
from utils.auth import get_current_user
from pdf2image import convert_from_bytes
from PIL import Image

from functools import wraps
from sqlalchemy import select, update


def counter_update(func):
    """
    счетчик проверок
    """

    @wraps(func)
    async def wrapped(self, *args, **kwargs):
        result = await func(self, *args, **kwargs)

        q = await self.db.execute(
            select(StatusCounter).where(StatusCounter.user_id == self.user_entity.user_id)
        )
        status_counter = q.scalars().first()
        if status_counter is None:
            status_counter = (await self.add_entity_to_db(
                StatusCounter, [{
                    'user_id': self.user_entity.user_id,
                    'counter': 0
                }]
            ))[0]
        status_counter.counter += 1
        self.db.add(status_counter)

        try:
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            print(str(e))

        finally:
            return result

    return wrapped


def log_end(func):
    """
    логгирование
    """

    @wraps(func)
    async def wrapped(self, *args, **kwargs):
        await func(self, *args, **kwargs)

        log_dict_upload = {
            'status': 'Обработка сертификата завершена',
            'user_id': self.user_entity.user_id,
        }

        logger.info(log_dict_upload, extra=log_dict_upload)

    return wrapped


@dataclass
class CertificateValidator:
    course_url: str = None
    course_title: str = None
    user_user_id: int = None

    async def celery_send(self, cert_id: str):
        log_dict_upload = {
            'status': 'Началась отправка на микросервис',
            'user_id': self.user_user_id,
            'certificate_id': cert_id,
            'sent_data': {
                'title': self.course_title,
                'course_url': self.course_url,
            }
        }
        celery_logger.info(log_dict_upload, extra=log_dict_upload)

        # отправка в celery
        try:
            result = check_course.apply_async(
                args=[{
                    'title': self.course_title,
                    'url': self.course_url,
                    'user_id': self.user_user_id
                }], queue="check_course_queue",
                retry=True,
                auto_ack=True,
                retry_policy={
                    'max_retries': 10,
                    'interval_start': 1,
                    'interval_step': 1,
                    'interval_max': 20,
                    'retry_errors': ('OperationalError',),  # Ошибки, по которым нужно повторять
                }
            )

            log_dict_upload['status'] = "Успешно отправлен на проверку на микросервис"
            del log_dict_upload['sent_data']

            celery_logger.info(log_dict_upload, extra=log_dict_upload)
        except Exception as e:
            log_dict_upload = {
                'status': 'Ошибка отправки сертификата на микросервис',
                'user_id': self.user_user_id,
                'certificate_id': cert_id,
                'error': f'{type(e).__name__} - {str(e)}'
            }

            celery_logger.error(log_dict_upload, extra=log_dict_upload)
            return {"verified": False, "error": "Ошибка сервера проверки"}

        res = await poll_task_result(result.id, user_id=self.user_user_id, cert_id=cert_id)

        return res


class CertificateManager:
    def __init__(self,
                 user_entity: User = Depends(get_current_user),
                 db: AsyncSession = Depends(get_session),
                 cert_uuid: Union[str, UUID] = None,
                 title: str = None,
                 course_url: str = None,
                 file: UploadFile = None,
                 file_cont: bytes = None):

        self.user_entity = user_entity
        self.db = db
        self.cert_uuid = cert_uuid
        self.title = title
        self.course_url = course_url
        self.file = file
        self.file_cont = file_cont

    allow_extension = ['pdf', 'jpg', 'jpeg']  # допустимые форматы файла
    LENGTH = 20

    @counter_update
    @log_end
    async def upload_certificate_file(self) -> (int, str):
        log_dict_upload = {
            'status': 'Началась обработка сертификата',
            'user_id': self.user_entity.user_id,
        }

        logger.info(log_dict_upload, extra=log_dict_upload)

        self.title = await self.decode_title()

        # формируем информацию о загружаемом файле
        detailed_status_id, file_info = await self.parse_file_information()

        # переключаем статус на НА проверке
        general_status_id = CertificateStatusEntry.VERIFYING[0]

        if detailed_status_id is not None:
            general_status_id = CertificateStatusEntry.REJECTED[0]

        # добавляем в БД информацию о загружаемом файле
        certificate_files = await self.add_entity_to_db(
            CertificateFile, [file_info],
            {
                'id': uuid4(),
                'created_date': datetime.now(timezone('Europe/Moscow')).replace(tzinfo=None),
                'general_status_id': general_status_id,
                'detailed_status_id': detailed_status_id,
                'front_status_id': detailed_status_id,
                'course_link': self.course_url
            }
        )

        cert_to_verify = certificate_files[0]

        self.db.add(cert_to_verify)

        try:
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            log_db_error(log_dict_upload, e)
            return

        if cert_to_verify.general_status_id == CertificateStatusEntry.REJECTED[0]:
            log_cert_rejected(log_dict_upload,
                              CertificateDetailedStatusEntry.EXTENSION_NOT_ALLOW[2],
                              str(cert_to_verify.id))
            return

        # отправка на микросервис, который парсит данные из загружаемого файла через celery
        url_validator = CertificateValidator(course_url=self.course_url,
                                             course_title=self.title,
                                             user_user_id=self.user_entity.user_id)

        res = await url_validator.celery_send(str(cert_to_verify.id))

        # проверка полученного результата и внесение данных в БД
        if res['verified'] is False:
            pass  # здесь функции проверки результата
        else:
            if len(res['skills_list']) != 0:
                user_terms = await self.parse_terms_from_resp(res['skills_list'])  # добавляем спаршенные данные в массив для дальнейшей обработки
            else:
                user_terms = []

        # формируем путь файла
        file_path = await self.get_file_path(
            file_info['filename'],
            file_info['extension'],
        )

        # загружаем файл в хранилище
        general_status_id, detailed_status_id = await self.upload_file_to_storage(
            file_content=file_info['file_content'],
            file_path=file_path
        )

        # переключение статусов проверки
        cert_to_verify.general_status_id = general_status_id
        cert_to_verify.detailed_status_id = detailed_status_id
        cert_to_verify.front_status_id = detailed_status_id
        self.db.add(cert_to_verify)

        await self.db.flush()

        # добавление всех необходимых данных в соответствующие таблицы БД
        # сохраняем бинарник файла в БД
        binary_files = await self.add_entity_to_db(BinaryCertificateFile, [{
            'file_id': cert_to_verify.id,
            'binary': self.file_cont
        }])

        self.db.add(binary_files[0])

        # добавляем спаршенные данные в таблицу привязки к конкретному пользователю
        user_term_level_files = await self.add_entity_to_db(
            UserTermLevelFile,
            user_terms,
            {
                'file_id': cert_to_verify.id,
                'user_id': self.user_entity.user_id,
            }
        )

        for utlfile in user_term_level_files:
            self.db.add(utlfile)

        await self.update_user_terms(user_terms)

        # переключаем статус проверки на Успех
        if cert_to_verify.general_status_id == CertificateStatusEntry.VERIFIED[0]:
            cert_to_verify.front_status_id = CertificateFrontStatusEntry.SUCCESS[0]
            self.db.add(cert_to_verify)

        try:
            await self.db.commit()

        except Exception as e:
            await self.db.rollback()
            log_dict_upload['certificate_id'] = str(cert_to_verify.id)
            log_db_error(log_dict_upload, e,
                         other_status='Ошибка сохранения сертификата в БД после всех проверок')

        finally:
            return

    async def decode_title(self):
        # декодирование title, т.к. в некоторых браузерах формат utf отправляется некорректно,
        # поэтому было внедрено решение кодировать тему в base64
        try:
            encoded_data = base64.b64decode(self.title).decode('utf-8')
            decoded_data = urllib.parse.unquote(encoded_data)
            return decoded_data
        except Exception:
            return self.title

    async def update_user_terms(self, terms: List[Dict]):
        # обновляем данные в БД
        pass

    async def parse_terms_from_resp(self, resp: List[Dict]) -> List[Dict]:
        # обрабатываем данные ответа с микросервиса и формируем массив словарей необходимого формата
        """
        [
        {'name': 'SQL',
        'id': 'e07ef774-08fb-4e49-b722-04624e12be68',
        'match_count': 10
        }, ...]
        """
        existing_terms_ids = (await self.db.execute(
            select(Term.id).distinct()
        )).scalars().all()

        user_terms = []

        for term in resp:
            if term['id'] in existing_terms_ids:
                term_info = {
                    'term_id': term['id'],
                    'match_count': term['match_count'],
                    'id': uuid4()
                }
                user_terms.append(term_info)

        return user_terms

    async def parse_file_information(self) -> Tuple[bool, Dict]:
        """
        Обрабатываем всю необходимую информацию о загружаемом файле
        :return: массив с ID статуса обработки и словарем с данными о файле
        """
        detailed_status_id = None

        path = pathlib.Path(self.file.filename)

        extension = path.suffix.replace('.', '')

        letter_and_digit = string.ascii_letters + string.digits

        filename = ''

        for i in range(0, self.LENGTH):
            filename += choice(letter_and_digit)  # присваиваем файлу уникальное имя

        try:
            # обрабатываем файл в виде изображения, чтобы сохранить иконку файла в БД
            success, img_icon = await self.build_image_icon(
                extension,
                (100, 100)
            )
            if success is False:
                img_icon = None
                detailed_status_id = CertificateDetailedStatusEntry.EXTENSION_NOT_ALLOW[0]
        except Exception:
            detailed_status_id = CertificateDetailedStatusEntry.IMAGE_ICON_ERROR[0]
            img_icon = None

        if extension.lower() not in self.allow_extension:
            detailed_status_id = CertificateDetailedStatusEntry.EXTENSION_NOT_ALLOW[0]

        return detailed_status_id, {
            'user_id': self.user_entity.user_id,
            'mime_type': self.file.content_type,
            'extension': extension,
            'original_filename': path.name,
            'checksum': hashlib.md5(self.file_cont).hexdigest(),
            'filename': filename,
            'file_content': self.file_cont,
            'image_icon': img_icon,
        }

    async def build_image_icon(self,
                               extension: str,
                               image_size: Tuple[int, int]
                               ) -> Union[Tuple[bool, bytes], Tuple[bool, Dict]]:
        # формируем иконку файла
        if extension.lower() == 'pdf':
            # Convert pdf bytes to images
            imgs = convert_from_bytes(self.file_cont, fmt="jpeg")

            if imgs:
                # We only need the first page
                first_page = imgs[0]

                # Resize the image
                resized_image = first_page.resize(image_size)
            else:
                return False, {'error': 'No images found'}

        elif extension.lower() in ['jpg', 'jpeg', 'png']:
            # забираем изображение из байтов
            img = Image.open(io.BytesIO(self.file_cont))

            # меняем размер изображения
            resized_image = img.resize(image_size)

        else:
            return False, {'error': 'Extension not correct'}

        # конвертируем объект иконки в байты
        image_byte_arr = io.BytesIO()
        resized_image.save(image_byte_arr, format='JPEG')
        image_byte_arr = image_byte_arr.getvalue()

        return True, image_byte_arr

    async def get_file_path(self,
                            filename: str,
                            extension: str,
                            ) -> str:
        # получаем путь файла
        upload_path = pathlib.Path().cwd() / 'upload' / f'{self.user_entity.user_id}'

        return os.path.join(
            upload_path,
            filename + '.' + extension
        )

    async def upload_file_to_storage(self,
                                     file_path: str,
                                     file_content: bytes) -> Tuple[Union[int, None], int]:
        # сохраняем файл в хранилище
        upload_path = pathlib.Path().cwd() / 'upload' / f'{self.user_entity.user_id}'

        # переключаем статусы
        if os.path.isfile(file_path):
            return CertificateStatusEntry.REJECTED[0],
        CertificateDetailedStatusEntry.FILE_PATH_ERROR[0]

        # Создаем директорию с правильными правами, если нужна
        if not os.path.exists(upload_path):
            os.makedirs(upload_path, mode=0o777)

        try:
            async with aiofiles.open(file_path, 'wb') as out_file:
                await out_file.write(file_content)
            return CertificateStatusEntry.VERIFIED[0], None
        except Exception:
            return CertificateStatusEntry.REJECTED[0],
            CertificateDetailedStatusEntry.FILE_PATH_ERROR[0]

    @staticmethod
    async def add_entity_to_db(entity_class: sqlalchemy.orm.decl_api.DeclarativeMeta,
                               additions: List[Dict],
                               extra_add: Dict = None) -> List[DeclarativeMeta]:
        """
        :param entity_class: Определяет класс, объект которого создается

        :param additions: представляет собой массив словарей, в которых хранятся ключи, значения которых присваиваются
        нужному атрибуту создаваемого объекта класса. Значения разные для всех создаваемых объектов!

        :param extra_add: представляет собой словарь с дополнительными значениями, которые присваиваются каждому создаваемому
        объекту модели. Необязательный параметр. Значения одинаковые для всех создаваемых объектов!
        """

        entities_list = []

        for addition in additions:
            entity_to_add = entity_class()  # объявляем объект класса
            for col_name in entity_class.__table__.columns.keys():  # получаем названия колонок класса
                if col_name in addition.keys():  # проверяем, есть ли такое название в ключе additions
                    entity_to_add.__setattr__(col_name, addition[col_name])  # присваиваем значение объекту класса

                if isinstance(extra_add, dict) and col_name in extra_add.keys():
                    entity_to_add.__setattr__(col_name, extra_add[col_name])

            entities_list.append(entity_to_add)

        return entities_list
