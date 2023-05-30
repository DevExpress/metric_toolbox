from collections.abc import Collection

class MockFilterParametersNode:

    def __init__(self, include: bool, values: Collection) -> None:
        self.include = include
        self.values = values


class MockFilterParameterNode:

    def __init__(self, include: bool, value: str | int) -> None:
        self.include = include
        self.value = value


class MockPercentile:

    def __init__(self, metric: str, value: MockFilterParameterNode) -> None:
        self.metric = metric
        self.value = value
