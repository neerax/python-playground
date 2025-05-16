class Tool:
    pass

class EventEmitter:
    def emit(self, message):
        print("[EMIT]:", message)