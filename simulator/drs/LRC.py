from simulator.drs.base import Base


class LRC(Base):
    """
    Only works when local parity is 1 (m0 = 1).
    """

    def __init__(self, params):
        self.n = int(params[0])
        self.k = int(params[1])
        self.ll = int(params[2])
        self.m0 = 1
        self.m1 = self.n - self.k - self.ll * self.m0

    def _check(self):
        if self.k < 0 or self.ll < 0 or self.m0 < 0 or self.m1 < 0:
            raise Exception("All coding parameters must be positive integer!")

    # Get local groups list.
    def _extract_local(self, state):
        b = self.k/self.ll
        local_groups = [state[x * b: (x + 1) * b] +
                        state[(self.k + x * self.m0):
                        (self.k + (x + 1) * self.m0)]
                        for x in xrange(self.ll)]
        return local_groups

    # Reverse function to _extrace_local()
    def _combine(self, local_groups):
        datas = []
        local_parities = []

        b = self.k/self.ll
        for item in local_groups:
            datas += item[:b]
            local_parities += item[b:]

        return datas + local_parities

    @property
    def isMDS(self):
        return False

    @property
    def ORC(self):
        return float(self.k)/float(self.ll)

    @property
    def threshold(self):
        """
        failure property when one stripe has n-k failures
        """
        if self.n == 10 and self.k == 6:
            return 0.143
        elif self.n == 16 and self.k == 12:
            return 0.138
        else:
            raise Exception("Not support")

    def repairTraffic(self, hier=False, d_racks=0):
        rt = float(self.m1*self.RC + (self.n-self.m1)*self.ORC)/self.n
        if not hier:
            return rt
        else:
            reduced = float(self.n)/d_racks - 1
            if reduced > rt:
                return float(0)
            return rt - reduced

    # state format: [data block states, local parity states in group1,
    # local parity in group 2, global parity blocks]
    def isRepairable(self, state):
        if isinstance(state, int):
            return False

        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)

        # all blocks is available, don't neeed repair
        if avails == self.n:
            return True

        if avails < self.k:
            return False

        local_groups = self._extract_local(state)

        # check global group after local repairs.
        global_group = []
        b = self.k/self.ll
        # avail_equ means equation number we can use for recovery.
        avail_equ = 0
        loss_amount = 0
        for group in local_groups:
            group_loss_amount = len(group) - group.count(1)
            if group_loss_amount <= self.m0:
                global_group += [1 for item in xrange(b)]
            else:
                avail_equ += self.m0
                loss_amount += group_loss_amount
                global_group += group[:b]

        global_parity = state[-self.m1:]
        avail_equ += self.m1
        loss_amount += self.m1 - global_parity.count(1)
        global_group += global_parity
        # Available equations are no less than loss blocks means repairable.
        if avail_equ < loss_amount:
            return False
        else:
            return True

    # If state needs both local repair and global repair, which one first?
    # Maybe we need another function, return the state after repair?
    # Tentatively, we return a tuple (repair_cost, state_after_repair)in
    #  this function, this is different with MDS codes.
    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == 1:
            raise Exception("index:" + str(index) + " in " +str(state) + " is normal state")

        optimal_flag = False
        b = self.k/self.ll
        if 0 <= index < b or self.k <= index < self.k+self.m0:
            group = state[0:b] + state[self.k:self.k+self.m0]
        elif b <= index < 2*b or self.k+self.m0 <= index < self.k+2*self.m0:
            group = state[b:2*b] + state[self.k+self.m0:self.k+2*self.m0]
        else:
            group = []

        if group.count(1) >= b:
            optimal_flag = True
        state[index] = 1
        if optimal_flag:
            return self.ORC
        else:
            return self.RC

    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_index = []
        for i in xrange(self.n):
            if state[i] == -1 or state[i] == -2:
                state[i] = 1
                repair_index.append(i)
            if not only_lost and state[i] == 0:
                state[i] = 1
                repair_index.append(i)

        repair_amount = len(repair_index)
        if repair_amount == 0:
            return 0
        elif repair_amount == 1 and repair_index[0] < self.n-self.m1:
            return self.ORC
        else:
            return self.RC + repair_amount - 1


if __name__ == "__main__":
    lrc = LRC([10, 6, 2])
    #print lrc.isMDS
    #print lrc.DSC
    #print lrc.SSC
    #print lrc.ORC
    #print lrc.RC
    state = [1]*10
    state[3] = 0
    state[4] = -1
    state[5] = -2
    print lrc.isRepairable(state)
    print lrc.repair(state, 4)
    print lrc.parallRepair(state, True)
    print lrc.repairTraffic(True, 3)
