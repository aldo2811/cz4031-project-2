class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class NodeCoverage(metaclass=Singleton):
    def __init__(self):
        self._plan_node_count = 0
        self._query_node_count = 0
        self._total_node_count = 0

    def inc_p(self):
        self._plan_node_count += 1
        self._total_node_count += 1

    def inc_q(self):
        self._query_node_count += 1

    def inc_t(self):
        self._total_node_count += 1

    def __str__(self):
        # TODO: query node should be compared to total node in query node, not in plan node
        return f"{self._plan_node_count / self._total_node_count * 100}%," \
               f" {self._query_node_count / self._total_node_count * 100}%"
