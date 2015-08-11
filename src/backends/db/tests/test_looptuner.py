# Copyright 2008-2015 Canonical
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# For further info, check  http://launchpad.net/filesync-server

"""looptuner.py tests.

These are the edge test cases that don't belong in the doctest.
"""

__metaclass__ = type

import math
import time

from unittest import TestCase

from mock import MagicMock, call
from zope.interface import implements

from backends.db.looptuner import (
    ITunableLoop,
    LoopTuner,
)


class MainException(Exception):
    """Exception raised from the main body of an ITunableLoop."""


class CleanupException(Exception):
    """Exception raised from the cleanup method of an ITunableLoop."""


class IsDoneException(Exception):
    """Exception raised from the isDone method of an ITunableLoop."""


class FailingLoop(object):
    """Some loop that fails."""
    implements(ITunableLoop)

    def __init__(self, fail_main=False, fail_cleanup=False):
        self.fail_main = fail_main
        self.fail_cleanup = fail_cleanup

    _done = False  # Set by __call__ to signal termination

    def isDone(self):
        """Return wether the flag is marked as done."""
        return self._done

    def __call__(self, chunk_size):
        self._done = True
        if self.fail_main:
            raise MainException()

    def cleanUp(self):
        """Fail to cleanup."""
        if self.fail_cleanup:
            raise CleanupException()


class NotATunableLoop(object):
    """Some loop that doesn't respect the interface."""

    def isDone(self):
        """Yes, it's done."""
        return True

    def __call__(self, chunk_size):
        "This never gets called"


class TestSomething(TestCase):
    """Simple tests for the LoopTuner."""

    def test_cleanup_exception_on_success(self):
        """Main task succeeded but cleanup failed.

        Exception from cleanup raised.
        """
        loop = FailingLoop(fail_cleanup=True)
        tuner = LoopTuner(loop, 5, log=MagicMock())
        self.assertRaises(CleanupException, tuner.run)
        tuner.log.exception.assert_has_no_calls()

    def test_cleanup_exception_on_failure(self):
        """Main task failed and cleanup also failed.

        Exception from cleanup is logged.
        Original exception from main task is raised.
        """
        loop = FailingLoop(fail_main=True, fail_cleanup=True)
        tuner = LoopTuner(loop, 5, log=MagicMock())
        self.assertRaises(MainException, tuner.run)
        tuner.log.exception.assert_called_once_with(
            'Unhandled exception in cleanUp')


class TestTunableLoops(TestCase):
    """Simple tests for the tunable loops."""

    def test_respects_tunable_loop_interface(self):
        """The LoopTuner requires the operation you define for it to implement
        the ITunableLoop interface."""
        loop = NotATunableLoop()
        self.assertRaises(AssertionError, LoopTuner, loop, 1, 10)


class Loop(object):
    """A base class for tunable loops for testing."""

    def __init__(self):
        self.mark = MagicMock()

    def mark_change(self, last_chunk_size, new_chunk_size):
        """The mark_change includes a logarithmic order-of-magnitude
        indication, where "no change" is zero, decreases are negative numbers,
        and increases show up as positive numbers.  The expectations for these
        numbers are based on observation, and could in principle still vary a
        bit with different CPU architectures, compilers used for the Python
        interpreter, etc."""
        if last_chunk_size is None:
            self.mark("start")
            return
        change = "same"
        if new_chunk_size > last_chunk_size:
            change = "increased"
        elif new_chunk_size < last_chunk_size:
            change = "decreased"
        ratio = new_chunk_size / last_chunk_size
        order_of_magnitude = math.log10(ratio)
        self.mark("%s (%.1f)" % (change, order_of_magnitude))


class PlannedLoop(Loop):
    """Here's a very simple ITunableLoop.  It receives a list of timings to
    simulate for its iterations (it finishes when the list runs out), and for
    every iteration, prints out what happened to its chunk size parameter."""

    implements(ITunableLoop)

    def __init__(self, timings):
        super(PlannedLoop, self).__init__()
        self.last_chunk_size = None
        self.iteration = 0
        self.timings = timings
        self.clock = 0

    def isDone(self):
        """Return wether it's done or not."""
        done = self.iteration >= len(self.timings)
        if done:
            self.mark("done")
        return done

    def __call__(self, chunk_size):
        self.mark_change(self.last_chunk_size, chunk_size)
        self.last_chunk_size = chunk_size
        self.clock += self.timings[self.iteration]
        self.iteration += 1


class TestTuner(LoopTuner):
    """We tweak LoopTuner to simulate the timings we gave the PlannedLoop. This
    is for testing only; in normal use you wouldn't need to subclass the
    LoopTuner."""

    def _time(self):
        """Return the operation clock as a float."""
        return float(self.operation.clock)


class TestChunkSizeConvergence(TestCase):
    """Tests for the chunk size convergence."""

    def test_trivial_case(self):
        """If there's nothing to do, nothing happens."""

        goal_seconds = 10.0
        body = PlannedLoop([])
        loop = TestTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('done'),
        ])

    def test_ideal_case(self):
        """A typical run using the ITunableLoop follows this pattern: the
        LoopTuner very conservatively starts out with the minimum chunk size,
        finds that its first iteration finishes well within its time goal, and
        starts jacking up the work per iteration until it nears the ideal time
        per iteration.  Due to practical variations, it keeps oscillating a
        bit."""

        goal_seconds = 10.0
        body = PlannedLoop([5, 7, 8, 9, 10, 11, 10, 9, 10, 9, 10, 11, 10])
        loop = TestTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.2)'),
            call('increased (0.1)'),
            call('increased (0.1)'),
            call('increased (0.0)'),
            call('same (0.0)'),
            call('decreased (-0.0)'),
            call('same (0.0)'),
            call('increased (0.0)'),
            call('same (0.0)'),
            call('increased (0.0)'),
            call('same (0.0)'),
            call('decreased (-0.0)'),
            call('done'),
        ])

    def test_slow_run(self):
        """If our iterations consistently exceed their time goal, we stay stuck
        at the minimum chunk size."""

        goal_seconds = 10.0
        body = PlannedLoop([15, 11, 16, 20, 14, 15, 10, 12, 15])
        loop = TestTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('done'),
        ])

    def test_typical_run(self):
        """What happens usually is that performance is relatively stable, so
        chunk size converges to a steady state, but there are occasional
        spikes.  When one chunk is suddenly very slow, the algorithm
        compensates for that so that if the drop in performance was a fluke,
        the next chunk falls well short of its time goal."""

        goal_seconds = 10.0
        body = PlannedLoop([5, 7, 8, 9, 10, 11, 9, 20, 7, 10, 10])
        loop = TestTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.2)'),
            call('increased (0.1)'),
            call('increased (0.1)'),
            call('increased (0.0)'),
            call('same (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.1)'),
            call('increased (0.1)'),
            call('same (0.0)'),
            call('done'),
        ])


class CostedLoop(Loop):
    """This variant of the LoopTuner simulates an overridable cost function."""
    implements(ITunableLoop)

    def __init__(self, cost_function, counter):
        super(CostedLoop, self).__init__()
        self.last_chunk_size = None
        self.iteration = 0
        self.cost_function = cost_function
        self.counter = counter
        self.clock = 0

    def isDone(self):
        """Return wether it's done or not."""
        done = (self.iteration >= self.counter)
        if done:
            self.mark("done")
        return done

    def __call__(self, chunk_size):
        self.mark_change(self.last_chunk_size, chunk_size)
        self.last_chunk_size = chunk_size
        self.iteration += 1

    def computeCost(self):
        """Compute the cost using the provided function."""
        return self.cost_function(self.last_chunk_size)


class CostedTuner(LoopTuner):
    """Some tuner with aditional costs to run."""

    def __init__(self, *argl, **argv):
        self.clock = 0
        LoopTuner.__init__(self, *argl, **argv)

    def _time(self):
        """Get the time from the clock member."""
        if self.operation.last_chunk_size is not None:
            self.clock += self.operation.computeCost()
        return self.clock


class TestCostFunctions(TestCase):
    """It's up to the ITunableLoop to define what "chunk size" really means.
    It's an arbitrary unit of work.  The only requirement for LoopTuner to do
    useful work is that on the whole, performance should tend to increase with
    chunk size.

    Here we illustrate how a tuned loop behaves with different cost functions
    governing the relationship between chunk size and chunk processing time."""

    def test_constant_cost(self):
        """We've already seen a constant-time loop body where every iteration
        took too much time, and we got stuck on the minimum chunk size.  Now we
        look at the converse case.

        If iterations consistently take less than the ideal time, the algorithm
        will "push the boundary," jacking up the workload until it manages to
        fill up the per-iteration goal time.  This is good if, for instance,
        the cost function is very flat, increasing very little with chunk size
        but with a relatively large constant overhead.  In that case, doing
        more work per iteration means more work done for every time we pay that
        constant overhead.  And usually, fewer iterations overall.

        Another case where chunk size may keep increasing is where chunk size
        turns out not to affect performance at all.  Chunk size is capped to
        stop it from spinning into infinity in that case, or if for some reason
        execution time should turn out to vary inversely with chunk size."""

        goal_seconds = 10.0
        body = CostedLoop((lambda c: goal_seconds / 2), 20)
        loop = CostedTuner(body, goal_seconds, 100, 1000)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.2)'),
            call('increased (0.2)'),
            call('increased (0.2)'),
            call('increased (0.2)'),
            call('increased (0.2)'),
            call('increased (0.1)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('done'),
        ])

    def test_linear_cost_without_constant(self):
        """The model behind LoopTuner assumes that the cost of an iteration
        will tend to increase as a linear function of chunk size.  Constant
        cost is a degenerate case of that; here we look at more meaningful
        linear functions.

        If cost function is purely linear with zero overhead, we approach our
        time goal asymptotically.  In principle we never quite get there."""

        goal_seconds = 10.0
        body = CostedLoop((lambda c: c / 20), 10)
        loop = CostedTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.2)'),
            call('increased (0.1)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('increased (0.0)'),
            call('done'),
        ])

        self.assertTrue(body.computeCost() < goal_seconds)
        self.assertTrue(body.computeCost() > goal_seconds * 0.9)

    def test_linear_cost_with_constant(self):
        """Here's a variant with a relatively flat linear cost function
        (25 units of work per second), plus a large constant overhead of half
        the time goal.  It does not achieve equilibrium in 10 iterations."""

        goal_seconds = 10.0
        body = CostedLoop((lambda c: (goal_seconds / 2) + (c / 25)), 10)
        loop = CostedTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.0)'), call('increased (0.0)'),
            call('increased (0.0)'), call('increased (0.0)'),
            call('increased (0.0)'), call('increased (0.0)'),
            call('increased (0.0)'), call('increased (0.0)'),
            call('increased (0.0)'),
            call('done'),
        ])

        self.assertTrue(body.computeCost() < goal_seconds)
        # But once again it does get pretty close
        self.assertTrue(body.computeCost() > goal_seconds * 0.9)

    def test_low_exponential_cost(self):
        """What if the relationship between chunk size and iteration time is
        much more radical?

        Due to the way LoopTuner's approximation function works, an exponential
        cost function will cause some oscillation where iteration time
        overshoots the goal, compensates, then finally converges towards it.

        If the cost function is highly regular and predictable, the oscillation
        will be a neat alternation of oversized and undersized chunks.

        We use math.pow here, since __builtin__.pow is overridden by
        twisted.conch."""

        goal_seconds = 10.0
        body = CostedLoop(lambda c: math.pow(1.2, c), 50)
        loop = CostedTuner(body, goal_seconds, 1)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.7)'),
            call('increased (0.4)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('done'),
        ])

    def test_high_exponential_cost(self):
        """With more extreme exponential behaviour, the overshoot increases but
        the effect remains the same:

        We use math.pow here, since __builtin__.pow is overridden by
        twisted.conch.

        Most practical algorithms will be closer to the linear cost function
        than they are to the exponential one."""

        goal_seconds = 10.0
        body = CostedLoop(lambda c: math.pow(3, c), 50)
        loop = CostedTuner(body, goal_seconds, 1)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('increased (0.3)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('increased (0.0)'),
            call('decreased (-0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'),
            call('increased (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'), call('same (0.0)'), call('same (0.0)'),
            call('same (0.0)'),
            call('done'),
        ])


class CooldownTuner(LoopTuner):
    """Some tuner that cools down."""

    def _coolDown(self, bedtime):
        if self.cooldown_time is None or self.cooldown_time <= 0.0:
            self.operation.mark("No cooldown")
        else:
            self.operation.mark("Cooldown for %.1f seconds." %
                                self.cooldown_time)
        return bedtime


class SimpleLoop(Loop):
    """SimpleLoop is a loop that does a constant number of iterations,
    regardless of the actual run-time."""

    implements(ITunableLoop)

    def __init__(self, iterations):
        super(SimpleLoop, self).__init__()
        self.total_iterations = iterations
        self.iteration = 0
        self.clock = 0

    def isDone(self):
        """Return wether it's done or not."""
        done = (self.iteration >= self.total_iterations)
        if done:
            self.mark("done")
        return done

    def __call__(self, chunk_size):
        self.mark("Processing %d items." % (chunk_size))
        self.iteration += 1


class TestLoopCooldown(TestCase):
    """LoopTuner allows inserting a delay between two consecutive operation
    runs.

    Overriding _coolDown method can be used to avoid an actual cooldown, but
    still print out what would happen."""

    def test_simple(self):
        """Aim for a low goal_seconds (to reduce test runtime), and only 3
        iterations."""

        goal_seconds = 0.01
        body = SimpleLoop(3)
        loop = CooldownTuner(body, goal_seconds, 1, cooldown_time=0.1)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('Processing 1 items.'),
            call('Cooldown for 0.1 seconds.'),
            call('Processing 5 items.'),
            call('Cooldown for 0.1 seconds.'),
            call('Processing 30 items.'),
            call('Cooldown for 0.1 seconds.'),
            call('done'),
        ])

    def test_cooldown_bedtime(self):
        """A private _coolDown method on LoopTuner sleeps for cooldown_time,
        and returns time after sleep is done."""

        goal_seconds = 0.01
        body = SimpleLoop(3)

        cooldown_loop = LoopTuner(body, goal_seconds, cooldown_time=0.2)
        old_time = time.time()

        new_time = cooldown_loop._coolDown(old_time)

        self.assertTrue(new_time > old_time)

    def test_without_cooldown_time(self):
        """If no cooldown_time is specified, there's no sleep, and exactly the
        same time is returned."""

        goal_seconds = 0.01
        body = SimpleLoop(3)

        no_cooldown_loop = LoopTuner(body, goal_seconds, cooldown_time=None)
        old_time = time.time()

        new_time = no_cooldown_loop._coolDown(old_time)

        self.assertEqual(new_time, old_time)


class TestTimeoutAbortion(TestCase):
    """Tests for the abort timeout."""

    def test_abortion(self):
        """LoopTuner allows a timeout to be specified. If the loop runs for
        longer than this timeout, it is aborted and a INFO logged."""

        goal_seconds = 0.01
        logger = MagicMock()
        body = PlannedLoop([5, 7, 8, 9, 10, 11, 9, 20, 7, 10, 10])
        loop = TestTuner(body, goal_seconds, 100, abort_time=20, log=logger)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('same (0.0)'),
            call('same (0.0)'),
        ])
        self.assertEqual(logger.info.mock_calls, [
            call('Task aborted after %d seconds.', 20),
        ])


class PlannedLoopWithCleanup(PlannedLoop):
    """Some planned loop that cleans up."""

    def cleanUp(self):
        """Mark as cleaned up."""
        self.mark('clean up')


class TestCleanup(TestCase):
    """Tests for the cleanup."""

    def test_cleanup(self):
        """Loops can define a clean up hook to clean up opened resources. We
        need this because loops can be aborted mid run, so we cannot rely on
        clean up code in the isDone() method, and __del__ is fragile and can
        never be relied on."""

        goal_seconds = 0.01
        body = PlannedLoopWithCleanup([])
        loop = TestTuner(body, goal_seconds, 100)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('done'),
            call('clean up'),
        ])

    def test_cleanup_even_if_aborted(self):
        """The cleanup kicks in even if the operation is aborted."""

        goal_seconds = 0.01
        logger = MagicMock()
        body = PlannedLoopWithCleanup([5, 7, 8, 9, 10, 11, 9, 20, 7, 10, 10])
        loop = TestTuner(body, goal_seconds, 100, abort_time=20, log=logger)

        loop.run()

        self.assertEqual(body.mark.mock_calls, [
            call('start'),
            call('same (0.0)'),
            call('same (0.0)'),
            call('clean up'),
        ])
        self.assertEqual(logger.info.mock_calls, [
            call('Task aborted after %d seconds.', 20),
        ])
