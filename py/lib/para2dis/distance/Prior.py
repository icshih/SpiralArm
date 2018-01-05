class Prior(object):
    """Various Prior properties for determining the distance, see Coryn Bailer-Jones, 2015, PASP, 127, 994"""

    def __init__(self):
        self.r_lim = 1.0

    def improper_uniform(self, distance):
        if distance > 0:
            return 1
        else:
            return 0

    def set_r_lim(self, r_lim):
        if r_lim <= 0:
            raise ValueError
        else:
            self.r_lim = r_lim

    def proper_uniform(self, distance):
        if 0 < distance <= self.r_lim:
            return 1 / self.r_lim
        else:
            return 0
