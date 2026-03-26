<?php

declare(strict_types=1);

namespace Allsilaevex\Pool;

use Throwable;
use WeakReference;
use LogicException;
use ReflectionClass;
use SplObjectStorage;
use Swoole\Coroutine;
use Psr\Log\NullLogger;
use Psr\Log\LoggerInterface;
use Swoole\Coroutine\Channel;
use Allsilaevex\Pool\Hook\PoolItemHook;
use Allsilaevex\Pool\Hook\PoolItemHookManagerInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskSchedulerInterface;

use function max;
use function hrtime;
use function is_null;
use function sprintf;
use function spl_object_id;
use function array_key_exists;

/**
 * @template TItem of object
 *
 * @implements PoolInterface<TItem>
 * @implements PoolControlInterface<TItem>
 */
final class Pool implements PoolInterface, PoolControlInterface
{
    protected PoolMetrics $metrics;

    /** @var SplObjectStorage<TItem, PoolItemWrapperInterface<TItem>> */
    protected SplObjectStorage $borrowedItemStorage;

    /** @var SplObjectStorage<PoolItemWrapperInterface<TItem>, int> */
    protected SplObjectStorage $idledItemStorage;

    /** @var Channel<PoolItemWrapperInterface<TItem>> */
    protected Channel $concurrentBag;

    /** @var array<int, TItem> */
    protected array $itemToCoroutineBindings;

    /** @var array<int, int> */
    protected array $borrowedItemOwnerCoroutineBindings;

    protected int $itemWrapperCount;

    /**
     * @param  non-empty-string                                               $name
     * @param  PoolItemWrapperFactoryInterface<TItem>                         $poolItemWrapperFactory
     * @param  TimerTaskSchedulerInterface<PoolControlInterface<TItem>>|null  $timerTaskScheduler
     * @param  PoolItemHookManagerInterface<TItem>|null                       $poolItemHookManager
     */
    public function __construct(
        protected string $name,
        protected PoolConfig $config,
        protected PoolItemWrapperFactoryInterface $poolItemWrapperFactory,
        protected LoggerInterface $logger = new NullLogger(),
        protected ?TimerTaskSchedulerInterface $timerTaskScheduler = null,
        protected ?PoolItemHookManagerInterface $poolItemHookManager = null,
    ) {
        $this->metrics = new PoolMetrics();
        $this->concurrentBag = new Channel($config->size);
        $this->itemWrapperCount = 0;
        $this->idledItemStorage = new SplObjectStorage();
        $this->borrowedItemStorage = new SplObjectStorage();
        $this->itemToCoroutineBindings = [];
        $this->borrowedItemOwnerCoroutineBindings = [];

        $this->timerTaskScheduler?->bindTo($this);
        $this->timerTaskScheduler?->run();

        $this->timerTaskScheduler?->start();
    }

    public function __destruct()
    {
        $this->timerTaskScheduler?->stop();

        $this->idledItemStorage->removeAll($this->idledItemStorage);

        $this->borrowedItemStorage->removeAll($this->borrowedItemStorage);

        $this->concurrentBag->close();
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function borrow(): mixed
    {
        $cid = Coroutine::getCid();

        if ($this->config->bindToCoroutine && array_key_exists($cid, $this->itemToCoroutineBindings)) {
            return $this->itemToCoroutineBindings[$cid];
        }

        $start = hrtime(true);

        while (true) {
            $elapsedSec = (((float) hrtime(true)) - ((float) $start)) / 1_000_000_000.0;

            if ($elapsedSec >= $this->config->borrowingTimeoutSec) {
                $this->metrics->borrowingTimeoutsTotal++;

                throw new Exceptions\BorrowTimeoutException('Can\'t get valid item from pool before timeout');
            }

            $poolItemWrapper = $this->getReservedPoolItemWrapperWithExistingItem(
                timeLeftSec: max(.0001, $this->config->borrowingTimeoutSec - $elapsedSec),
                increaseItemsOnEmptyPool: true,
            );

            try {
                $this->poolItemHookManager?->run(PoolItemHook::BEFORE_BORROW, $poolItemWrapper);
            } catch (Throwable $throwable) {
                $this->removePoolItemWrapper($poolItemWrapper);

                $this->logger->error(
                    sprintf(
                        'Can\'t prepare item for borrowing (%s): %s',
                        (new ReflectionClass($throwable))->getShortName(),
                        $throwable->getMessage(),
                    ),
                    ['pool_name' => $this->getName(), 'item_id' => $poolItemWrapper->getId()],
                );

                continue;
            }

            if (!$poolItemWrapper->compareAndSetState(PoolItemState::RESERVED, PoolItemState::IN_USE)) {
                throw new LogicException();
            }

            $item = $poolItemWrapper->getItem();

            if (is_null($item)) {
                $this->removePoolItemWrapper($poolItemWrapper);

                continue;
            }

            $this->idledItemStorage->offsetUnset($poolItemWrapper);
            $this->borrowedItemStorage->offsetSet($item, $poolItemWrapper);
            $this->borrowedItemOwnerCoroutineBindings[spl_object_id($item)] = $cid;

            if ($this->config->bindToCoroutine) {
                $this->itemToCoroutineBindings[$cid] = $item;
            }

            if ($this->config->autoReturn) {
                $itemRef = WeakReference::create($item);

                Coroutine::defer(function () use ($cid, $itemRef) {
                    unset($this->itemToCoroutineBindings[$cid]);

                    $item = $itemRef->get();

                    $this->return($item);
                });
            }

            $this->metrics->borrowedTotal++;
            $this->metrics->waitingForItemBorrowingTotalSec += ((((float) hrtime(true)) - ((float) $start))) / 1_000_000_000.0;

            return $item;
        }
    }

    /**
     * @inheritDoc
     * @phpstan-param-out null $poolItemRef
     */
    #[\Override]
    public function return(mixed &$poolItemRef): void
    {
        $poolItemWrapper = $this->returnBorrowedItem($poolItemRef);

        if (is_null($poolItemWrapper)) {
            return;
        }

        if ($this->concurrentBag->isFull()) {
            return;
        }

        $this->metrics->itemInUseTotalSec += $poolItemWrapper->stats()['current_state_duration_sec'];

        if (is_null($this->poolItemHookManager)) {
            $poolItemWrapper->setState(PoolItemState::IDLE);
        } else {
            $poolItemWrapper->setState(PoolItemState::RESERVED);

            $this->poolItemHookManager->run(PoolItemHook::AFTER_RETURN, $poolItemWrapper);

            if (!$poolItemWrapper->compareAndSetState(PoolItemState::RESERVED, PoolItemState::IDLE)) {
                throw new LogicException();
            }
        }

        $this->idledItemStorage->offsetSet($poolItemWrapper, (int) hrtime(true));

        $isReturned = $this->concurrentBag->push($poolItemWrapper, $this->config->returningTimeoutSec);

        if (!$isReturned) {
            $this->idledItemStorage->offsetUnset($poolItemWrapper);
        }
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function stats(): array
    {
        return [
            'all_item_count' => $this->getCurrentSize(),
            'idled_item_count' => $this->getIdleCount(),
            'borrowed_item_count' => $this->borrowedItemStorage->count(),
            /** @phpstan-ignore-next-line */
            'consumer_pending_count' => (int)$this->concurrentBag->stats()['consumer_num'],

            'borrowed_total' => $this->metrics->borrowedTotal,
            'item_created_total' => $this->metrics->itemCreatedTotal,
            'item_deleted_total' => $this->metrics->itemDeletedTotal,
            'borrowing_timeouts_total' => $this->metrics->borrowingTimeoutsTotal,

            'item_in_use_total_sec' => $this->metrics->itemInUseTotalSec,
            'item_creation_total_sec' => $this->metrics->itemCreationTotalSec,
            'waiting_for_item_borrowing_total_sec' => $this->metrics->waitingForItemBorrowingTotalSec,
        ];
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function getName(): string
    {
        return $this->name;
    }

    #[\Override]
    public function getIdleCount(): int
    {
        return $this->concurrentBag->length();
    }

    #[\Override]
    public function getCurrentSize(): int
    {
        return $this->itemWrapperCount;
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function getIdledItemStorage(): SplObjectStorage
    {
        return $this->idledItemStorage;
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function getBorrowedItemStorage(): SplObjectStorage
    {
        return $this->borrowedItemStorage;
    }

    #[\Override]
    public function getConfig(): PoolConfig
    {
        return $this->config;
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function increaseItems(): bool
    {
        if ($this->concurrentBag->isFull()) {
            return false;
        }

        $this->itemWrapperCount++;

        $start = hrtime(true);

        try {
            $poolItemWrapper = $this->poolItemWrapperFactory->create();
        } catch (Throwable $throwable) {
            $this->itemWrapperCount--;
            throw $throwable;
        }

        $this->metrics->itemCreatedTotal++;
        $this->metrics->itemCreationTotalSec += ((((float) hrtime(true)) - ((float) $start))) / 1_000_000_000.0;

        $this->idledItemStorage->offsetSet($poolItemWrapper, (int) hrtime(true));

        $result = $this->concurrentBag->push($poolItemWrapper, .001);

        if ($result === false) {
            $this->removePoolItemWrapper($poolItemWrapper);
        }

        return $result;
    }

    #[\Override]
    public function decreaseItems(): bool
    {
        if ($this->concurrentBag->isEmpty()) {
            return false;
        }

        /** @var PoolItemWrapperInterface<TItem>|false $poolItemWrapper */
        $poolItemWrapper = $this->concurrentBag->pop(.001);

        if ($poolItemWrapper === false) {
            return false;
        }

        $this->removePoolItemWrapper($poolItemWrapper);

        return true;
    }

    /**
     * @inheritDoc
     * @phpstan-param-out null $poolItemRef
     */
    #[\Override]
    public function removeItem(mixed &$poolItemRef): void
    {
        $poolItemWrapper = $this->returnBorrowedItem($poolItemRef);

        if (is_null($poolItemWrapper)) {
            return;
        }

        $this->metrics->itemInUseTotalSec += $poolItemWrapper->stats()['current_state_duration_sec'];

        $this->removePoolItemWrapper($poolItemWrapper);
    }

    /**
     * @param  TItem|null  $poolItemRef
     * @phpstan-param-out null $poolItemRef
     *
     * @return PoolItemWrapperInterface<TItem>|null
     */
    protected function returnBorrowedItem(mixed &$poolItemRef): ?PoolItemWrapperInterface
    {
        if ($poolItemRef === null) {
            return null;
        }

        $poolItem = $poolItemRef;
        $poolItemRef = null;

        if (!$this->borrowedItemStorage->offsetExists($poolItem)) {
            return null;
        }

        /** @var PoolItemWrapperInterface<TItem> $poolItemWrapper */
        $poolItemWrapper = $this->borrowedItemStorage[$poolItem];

        $this->borrowedItemStorage->offsetUnset($poolItem);

        $ownerCid = $this->borrowedItemOwnerCoroutineBindings[spl_object_id($poolItem)] ?? Coroutine::getCid();

        unset($this->borrowedItemOwnerCoroutineBindings[spl_object_id($poolItem)]);
        unset($this->itemToCoroutineBindings[$ownerCid]);

        if ($poolItemWrapper->getState() !== PoolItemState::IN_USE) {
            throw new LogicException();
        }

        return $poolItemWrapper;
    }

    /**
     * @param  PoolItemWrapperInterface<TItem>  $poolItemWrapper
     */
    protected function removePoolItemWrapper(PoolItemWrapperInterface $poolItemWrapper): void
    {
        $this->idledItemStorage->offsetUnset($poolItemWrapper);

        $poolItemWrapper->close();

        $this->itemWrapperCount--;
        $this->metrics->itemDeletedTotal++;
    }

    /**
     * @return PoolItemWrapperInterface<TItem>
     * @throws Exceptions\BorrowTimeoutException
     */
    protected function getPoolItemWrapper(float $timeLeftSec, bool $increaseItemsOnEmptyPool): PoolItemWrapperInterface
    {
        $isPoolEmpty = $this->concurrentBag->isEmpty() && $this->getCurrentSize() < $this->config->size;

        if ($increaseItemsOnEmptyPool && $isPoolEmpty) {
            \Swoole\Coroutine\go(function () {
                try {
                    $this->increaseItems();
                } catch (Throwable $exception) {
                    $errorMessage = sprintf(
                        'Can\'t create new item for empty pool (%s): %s',
                        (new ReflectionClass($exception))->getShortName(),
                        $exception->getMessage(),
                    );

                    $this->logger->error($errorMessage, ['pool_name' => $this->getName()]);
                }
            });
        }

        /** @var PoolItemWrapperInterface<TItem>|false $poolItemWrapper */
        $poolItemWrapper = $this->concurrentBag->pop($timeLeftSec);

        if ($poolItemWrapper === false) {
            $this->metrics->borrowingTimeoutsTotal++;

            throw new Exceptions\BorrowTimeoutException('Can\'t pop item from concurrentBag');
        }

        return $poolItemWrapper;
    }

    /**
     * @return PoolItemWrapperInterface<TItem>
     * @throws Exceptions\BorrowTimeoutException
     */
    protected function getReservedPoolItemWrapper(float $timeoutSec, bool $increaseItemsOnEmptyPool): PoolItemWrapperInterface
    {
        $start = hrtime(true);
        $poolItemWrapper = $this->getPoolItemWrapper($timeoutSec, $increaseItemsOnEmptyPool);
        $timeoutSec = max(.0001, $timeoutSec - ((((float) hrtime(true)) - ((float) $start)) / 1_000_000_000.0));

        if (!$poolItemWrapper->waitForCompareAndSetState(PoolItemState::IDLE, PoolItemState::RESERVED, $timeoutSec)) {
            $context = [
                'pool_name' => $this->getName(),
                'item_id' => $poolItemWrapper->getId(),
                'item_old_state' => $poolItemWrapper->getState()->name,
                'item_new_state' => PoolItemState::RESERVED->name,
            ];
            $errorMessage = sprintf(
                'Can\'t set %s state (old state %s)',
                PoolItemState::RESERVED->name,
                $poolItemWrapper->getState()->name,
            );

            $this->logger->error($errorMessage, $context);

            $this->metrics->borrowingTimeoutsTotal++;

            $result = $this->concurrentBag->push($poolItemWrapper, .001);

            if ($result === false) {
                $this->removePoolItemWrapper($poolItemWrapper);
            }

            throw new Exceptions\BorrowTimeoutException($errorMessage);
        }

        return $poolItemWrapper;
    }

    /**
     * @return PoolItemWrapperInterface<TItem>
     * @throws Exceptions\BorrowTimeoutException
     */
    protected function getReservedPoolItemWrapperWithExistingItem(float $timeLeftSec, bool $increaseItemsOnEmptyPool): PoolItemWrapperInterface
    {
        $start = hrtime(true);
        $poolItemWrapper = $this->getReservedPoolItemWrapper($timeLeftSec, $increaseItemsOnEmptyPool);

        if (is_null($poolItemWrapper->getItem())) {
            $this->removePoolItemWrapper($poolItemWrapper);

            $recalculatedTimeLeftSec = max(.0001, $timeLeftSec - ((((float) hrtime(true)) - ((float) $start)) / 1_000_000_000.0));

            return $this->getReservedPoolItemWrapperWithExistingItem($recalculatedTimeLeftSec, increaseItemsOnEmptyPool: false);
        }

        return $poolItemWrapper;
    }
}
