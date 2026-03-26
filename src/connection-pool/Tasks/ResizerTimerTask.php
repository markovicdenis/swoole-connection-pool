<?php

declare(strict_types=1);

namespace Allsilaevex\ConnectionPool\Tasks;

use Throwable;
use Psr\Log\LoggerInterface;
use Allsilaevex\Pool\PoolControlInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskSchedulerAwareTrait;

use function hrtime;
use function is_null;

/**
 * @template TItem of object
 *
 * @implements TimerTaskInterface<PoolControlInterface<TItem>>
 */
final class ResizerTimerTask implements TimerTaskInterface
{
    /** @phpstan-use TimerTaskSchedulerAwareTrait<PoolControlInterface<TItem>> */
    use TimerTaskSchedulerAwareTrait;

    public function __construct(
        public readonly float $intervalSec,
        public readonly int $minimumIdle,
        public readonly float $idleTimeoutSec,
        public readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function run(int $timerId, mixed $runnerRef): void
    {
        /** @var PoolControlInterface<TItem>|null $runner */
        $runner = $runnerRef->get();

        if (is_null($runner)) {
            return;
        }

        if ($runner->getCurrentSize() > 0 && $runner->getConfig()->size === $this->minimumIdle) {
            $this->timerTaskSchedulerRef?->get()?->stopTask($timerId);

            return;
        }

        while ($runner->getCurrentSize() < $runner->getConfig()->size && $runner->getIdleCount() < $this->minimumIdle) {
            try {
                $runner->increaseItems();
            } catch (Throwable $exception) {
                $this->logger->error('Can\'t create new connection: ' . $exception->getMessage(), ['pool_name' => $runner->getName()]);

                return;
            }
        }

        if ($runner->getIdleCount() > $this->minimumIdle) {
            $now = hrtime(true);
            $idleItemCount = 0;
            $idledItemStorage = $runner->getIdledItemStorage();

            $idledItemStorage->rewind();

            while ($idledItemStorage->valid()) {
                $time = $idledItemStorage->getInfo();

                if ((((float) $now) - ((float) $time)) / 1_000_000_000.0 > $this->idleTimeoutSec) {
                    $idleItemCount++;
                }

                $idledItemStorage->next();
            }

            while ($idleItemCount-- !== 0 && $runner->getIdleCount() > $this->minimumIdle) {
                $runner->decreaseItems();
            }
        }
    }

    #[\Override]
    public function getIntervalSec(): float
    {
        return $this->intervalSec;
    }
}
