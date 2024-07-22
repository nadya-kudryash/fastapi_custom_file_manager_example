import asyncio

from celery import Celery


class Config(object):
    result_backend = f'rpc://{RABBIT_MQ_HOST}:5672'
    broker_url = f'amqp://{RABBIT_MQ_USERNAME}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_HOST}:5672'
    broker_connection_retry_on_startup = True
    CELERY_ENABLE_UTC = True
    TASK_ROUTES = {
        'check_course': {'queue': 'check_course_queue'},
    }
    result_persistent = True
    result_expires = 72000
    worker_cancel_long_running_tasks_on_connection_loss = True


celery_app = Celery('tasks')
celery_app.config_from_object(Config)


@celery_app.task(name='check_course')
async def check_course(message):
    pass


async def poll_task_result(task_id: str, unti_id: int, cert_id: str):
    """
    Поллинг результата парсинга
    """
    log_dict_upload = {
        'status': 'Поллим выполнение запроса на микросервис...',
        'unti_id': unti_id,
        'certificate_id': cert_id,
        'task_id': task_id,
    }
    celery_logger.info(log_dict_upload, extra=log_dict_upload)

    while True:
        task = check_course.AsyncResult(task_id)
        if task.state == 'SUCCESS':
            result = task.result
            log_dict_upload['status'] = 'Результат проверки микросервисом получен'
            log_dict_upload['result'] = result
            celery_logger.info(log_dict_upload, extra=log_dict_upload)
            return result
        elif task.state == 'FAILURE':
            log_dict_upload['status'] = 'Ошибка проверки микросервисом'
            log_dict_upload['error'] = task.args
            celery_logger.error(log_dict_upload, extra=log_dict_upload)

            return {
                'verified': False,
                'error': 'Ошибка сервера проверки'
            }
        await asyncio.sleep(1)
