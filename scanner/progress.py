from time import perf_counter


class Counter:
    def __init__(self, name: str, stdout_every: int):
        self.name = name
        self.count = 0
        self.sub_counter = 0
        self.stdout_every = stdout_every

    def __call__(self, increment_by: int=1):
        self.count += increment_by
        self.sub_counter += increment_by
        if self.sub_counter >= self.stdout_every:
            self.sub_counter = 0
            print(f'\r{self.name}: {self.count:,}', end='', flush=True)


class Perf:
    __slots__ = ('previous', 'checkpoints', 'topic', 'enabled')

    def __init__(self, topic: str=None, enabled=False):
        self.enabled = enabled
        if self.enabled:
            self.topic = topic
            self.checkpoints = []
            self.previous = perf_counter()

    def check(self, name: str=""):
        if self.enabled:
            now = perf_counter()
            elapsed = now - self.previous
            self.previous = now
            self.checkpoints.append((name, elapsed))

    def __repr__(self) -> str:
        if self.enabled:
            total = sum(point[1] for point in self.checkpoints)
            longest = max(max(len(point[0]) for point in self.checkpoints), 5) # 5 is len of 'total'
            topic = f'[{self.topic}]\n' if self.topic else ''
            return topic + '\n'.join(
                f'{name:<{longest}}: {elapsed:.4f} {elapsed / total * 100 :.1f}%'
                for name, elapsed in self.checkpoints
            ) + f'\n{"total":<{longest}}: {total:.4f}'
        else:
            return ''
