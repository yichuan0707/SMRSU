

class Result(object):
    lost_slice_count = 0
    unavailable_slice_count = 0

    PDL = 0.0  # PDL = (lost slices)/(total slices)
    PDLT = 0.0  # PDLT = (lost times)/(total slices)
    PUA = 0.0  # PUA = (unavailable period)/(mission time)
    PUAW = 0.0  # PUAW = (unavailable duration* unavailable slices)/(total slices * mission time)

    # total repair cost
    TRT = 0.0

    def toString(self):
        return "lost=" + str(Result.lost_slice_count) + \
            "; unavailable=" + str(Result.unavailable_slice_count) + \
            "; PDL=" + str(Result.PDL) + \
            "; PDLT=" + str(Result.PDLT) + \
            "; PUA=" + str(Result.PUA) + \
            "; PUAW=" + str(Result.PUAW) + \
            "; TRT=" + str(Result.TRT) + "TiB"


if __name__ == "__main__":
    r = Result()
    print r.toString()
