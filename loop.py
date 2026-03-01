import time

import configs


class Loop:
    '''Loop mechanisms, and stats.'''
    def __init__(self):
        self.loop_i: int = 1
        self.start_time: float | None = None
        self.board_2_duration: dict[str, float] = dict()
        configs.logger.info(f'Loop #{self.loop_i} Started')

    @property
    def is_first_loop(self) -> bool:
        return self.loop_i == 1

    def set_start_time(self):
        self.start_time = time.time()

    def get_duration_minutes(self) -> float:
        return round((time.time() - self.start_time) / 60, 2)

    def set_board_duration_minutes(self, board: str):
        self.board_2_duration[board] = self.get_duration_minutes()

    def log_board_durations(self):
        s = 'Duration for each board:\n'

        for board, duration in self.board_2_duration.items():
            s += f'    - {board:<4} {duration:.1f}m\n'

        total_duration = round(sum(self.board_2_duration.values()), 1)
        s += f'Total Duration: {total_duration}m\n'
        configs.logger.info(s)

    def increment_loop(self):
        configs.logger.info(f'Loop #{self.loop_i} Completed\n')
        self.loop_i += 1

    def sleep(self):
        configs.logger.info(f'Doing loop cooldown sleep for {configs.loop_cooldown_sec}s\n')
        time.sleep(configs.loop_cooldown_sec)

