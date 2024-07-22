from utils.CertificateManager import CertificateManager


@router.post('', summary="Загрузка сертификата пользователем")
async def upload_certificate(background_tasks: BackgroundTasks,
                             body: UploadCertificateSchema,
                             file: UploadFile = File(...),
                             db: AsyncSession = Depends(get_session),
                             cur_user: User = Depends(get_current_user),
                             ):
    file_content = await file.read()  # нужно для обработки иконки файла

    certificate_manager = CertificateManager(
        user_entity=cur_user,
        db=db,
        title=body.title,
        course_url=body.url,
        file=file,
        file_cont=file_content
    )

    background_tasks.add_task(
        certificate_manager.upload_certificate_file)  # уводим задачу в фон, чтобы пользователь мог покинуть страницу

    return JSONResponse(status_code=200,
                        content='Сертификат отправлен на модерацию')
