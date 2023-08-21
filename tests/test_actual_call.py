import json
import socket

import httpretty

from conductor_celery.tasks import ConductorTask


# def test_conductor_task_case(celery_app, celery_worker):


#     @celery_app.task(base=ConductorTask, name="legal_entity_enrich_person")
#     def mul():
#         return {"total": 2}

#     celery_worker.reload()
#     assert mul.apply().result == {"total": 8}


