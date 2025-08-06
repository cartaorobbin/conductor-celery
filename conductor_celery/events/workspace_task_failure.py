from conductor_celery.stream.event import KafkaEvent


class WorkspaceTaskFailure(KafkaEvent):
    def __init__(self, request, payload, **kwargs):
        topic = "workspace_conductor_task_failures"
        super().__init__(request, topic, **payload)