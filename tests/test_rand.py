import multiprocessing
from ddtrace.internal import _rand


def test_random():
    m = set()
    for i in range(0, 2 ** 16):
        n = _rand.rand64bits()
        assert 0 <= n <= 2 ** 64 - 1
        assert n not in m
        m.add(n)


def test_random_fork():
    num_procs = 10

    q = multiprocessing.Queue()

    def target(q):
        q.put(_rand.rand64bits())

    for _ in range(num_procs):
        p = multiprocessing.Process(target=target, args=(q,))
        p.start()
        p.join()

    assert q.qsize() == num_procs
    nums = set([_rand.rand64bits()])
    while not q.empty():
        n = q.get()
        assert n not in nums
        nums.add(n)
