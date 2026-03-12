class StepError(Exception):
    def __init__(self, msg: str, *, code: str | None = None, advice: str | None = None):
        super().__init__(msg)
        self.code = code
        self.advice = advice


class PermanentStepError(StepError): ...


class TransientStepError(StepError): ...
