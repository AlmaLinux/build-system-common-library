import threading


class BaseSupervisor(threading.Thread):

    def __init__(self, config, builders, terminated_event):
        self.config = config
        self.builders = builders
        self.terminated_event = terminated_event
        super().__init__(name='BuildersSupervisor')

    def get_active_tasks(self):
        return set([b.current_task_id for b in self.builders]) - set([
            None,
        ])

    def run(self):
        raise NotImplementedError('Needs to be implemented in child classes')
