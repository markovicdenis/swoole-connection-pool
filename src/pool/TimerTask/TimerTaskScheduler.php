<?php

declare(strict_types=1);

namespace Allsilaevex\Pool\TimerTask;

use Swoole\Timer;
use WeakReference;
use LogicException;
use Allsilaevex\Pool\Exceptions\TimerTickScheduleException;

use function count;
use function round;
use function is_null;
use function method_exists;

/**
 * @template TRunner of object
 * @implements TimerTaskSchedulerInterface<TRunner>
 */
final class TimerTaskScheduler implements TimerTaskSchedulerInterface
{
    /** @var WeakReference<TRunner>|null  */
    protected ?WeakReference $runnerRef;

    /** @var list<int> */
    protected array $timerTaskIds;

    /**
     * @param  list<TimerTaskInterface<TRunner>>  $timerTasks
     */
    public function __construct(
        protected readonly array $timerTasks,
    ) {
        $this->runnerRef = null;
        $this->timerTaskIds = [];

        foreach ($this->timerTasks as $timerTask) {
            /** @see TimerTaskSchedulerAwareTrait */
            if (method_exists($timerTask, 'setTimerTaskScheduler')) {
                $timerTask->setTimerTaskScheduler($this);
            }
        }
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function bindTo(object $runner): void
    {
        $timerTaskIdCount = count($this->timerTaskIds);

        if ($timerTaskIdCount > 0) {
            $this->stop();
        }

        $this->runnerRef = WeakReference::create($runner);

        if ($timerTaskIdCount > 0) {
            $this->start();
        }
    }

    #[\Override]
    public function run(): void
    {
        if (is_null($this->runnerRef)) {
            throw new LogicException('Runner hasn\'t been bound to scheduler yet');
        }

        foreach ($this->timerTasks as $timerTask) {
            $timerTask->run(-1, $this->runnerRef);
        }
    }

    #[\Override]
    public function start(): void
    {
        if (is_null($this->runnerRef)) {
            throw new LogicException('Runner hasn\'t been bound to scheduler yet');
        }

        foreach ($this->timerTasks as $timerTask) {
            $timerId = Timer::tick((int) round($timerTask->getIntervalSec() * 1000.0), $timerTask->run(...), $this->runnerRef);

            if ($timerId === false) {
                throw new TimerTickScheduleException();
            }

            $this->timerTaskIds[] = $timerId;
        }
    }

    #[\Override]
    public function stopTask(int $timerId): bool
    {
        return Timer::clear($timerId);
    }

    #[\Override]
    public function stop(): void
    {
        foreach ($this->timerTaskIds as $timerId) {
            Timer::clear($timerId);
        }

        $this->timerTaskIds = [];
    }
}
