class TrialCounter:
    def __init__(self):
        self.total = 0
        self.success = 0

    def record_outcome(self, is_success):
        self.success += is_success
        self.total += 1

    def success_rate(self):
        return self.success / self.total

    def __repr__(self):
        return '%f %d/%d' % (self.success / self.total, self.success, self.total)
