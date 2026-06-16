<?php

declare(strict_types=1);

namespace Allsilaevex\ConnectionPool;

use LogicException;
use ReflectionClass;
use Psr\Log\NullLogger;
use Allsilaevex\Pool\Pool;
use Psr\Log\LoggerInterface;
use Allsilaevex\Pool\PoolConfig;
use Allsilaevex\Pool\PoolInterface;
use Allsilaevex\Pool\PoolItemWrapperFactory;
use Allsilaevex\Pool\Hook\PoolItemHookManager;
use Allsilaevex\Pool\PoolItemFactoryInterface;
use Allsilaevex\Pool\PoolItemWrapperInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskScheduler;
use Allsilaevex\ConnectionPool\Tasks\ResizerTimerTask;
use Allsilaevex\ConnectionPool\Hooks\ConnectionCheckHook;
use Allsilaevex\ConnectionPool\Tasks\LeakDetectionTimerTask;
use Allsilaevex\ConnectionPool\Tasks\KeepaliveCheckTimerTask;
use Allsilaevex\ConnectionPool\Tasks\PoolItemUpdaterTimerTask;

use function count;
use function substr;
use function uniqid;
use function array_map;

/**
 * @template TConnection of object
 * @phpstan-consistent-constructor
 * @psalm-consistent-constructor
 * @psalm-consistent-templates
 */
class ConnectionPoolFactory
{
    /** @var int<0, max> */
    protected int $minimumIdle;
    protected bool $autoReturn;
    protected bool $bindToCoroutine;
    protected float $idleTimeoutSec;
    protected float $maxLifetimeSec;
    protected float $borrowingTimeoutSec;
    protected float $returningTimeoutSec;
    protected float $leakDetectionThresholdSec;
    protected float $maxItemReservingForUpdateWaitingTimeSec;

    /** @var list<callable(TConnection): bool> */
    protected array $checkers;

    protected LoggerInterface $logger;

    /** @var list<KeepaliveCheckerInterface<TConnection>> */
    protected array $keepaliveCheckers;

    /** @var list<TimerTaskInterface<\Allsilaevex\Pool\PoolControlInterface<TConnection>>> */
    protected array $poolTimerTasks;

    /**
     * @param  positive-int                           $size
     * @param  PoolItemFactoryInterface<TConnection>  $factory
     */
    public function __construct(
        protected int $size,
        protected PoolItemFactoryInterface $factory,
    ) {
        $this->checkers = [];
        $this->logger = new NullLogger();
        $this->keepaliveCheckers = [];
        $this->poolTimerTasks = [];

        $this->minimumIdle = $this->size;
        $this->autoReturn = true;
        $this->bindToCoroutine = true;
        $this->idleTimeoutSec = 30.0;
        $this->maxLifetimeSec = 300.0;
        $this->borrowingTimeoutSec = .1;
        $this->returningTimeoutSec = .001;
        $this->leakDetectionThresholdSec = 1.0;
        $this->maxItemReservingForUpdateWaitingTimeSec = .01;
    }

    /**
     * @template TConnectionNew of object
     *
     * @param  positive-int                                $size
     * @param  PoolItemFactoryInterface<TConnectionNew>  $factory
     *
     * @return static<TConnectionNew>
     */
    public static function create(int $size, PoolItemFactoryInterface $factory): static
    {
        /** @psalm-suppress UnsafeGenericInstantiation */
        $connectionPoolFactory = new static($size, $factory);

        return $connectionPoolFactory;
    }

    public function setLogger(LoggerInterface $logger): static
    {
        $this->logger = $logger;

        return $this;
    }

    public function setLeakDetectionThresholdSec(float $leakDetectionThresholdSec): static
    {
        $this->leakDetectionThresholdSec = $leakDetectionThresholdSec;

        return $this;
    }

    public function setMaxItemReservingForUpdateWaitingTimeSec(float $maxItemReservingForUpdateWaitingTimeSec): static
    {
        $this->maxItemReservingForUpdateWaitingTimeSec = $maxItemReservingForUpdateWaitingTimeSec;

        return $this;
    }

    public function setAutoReturn(bool $autoReturn): static
    {
        $this->autoReturn = $autoReturn;

        return $this;
    }

    public function setBindToCoroutine(bool $bindToCoroutine): static
    {
        $this->bindToCoroutine = $bindToCoroutine;

        return $this;
    }

    /**
     * @param  int<0, max>  $minimumIdle
     *
     */
    public function setMinimumIdle(int $minimumIdle): static
    {
        // @phpstan-ignore smaller.alwaysFalse
        if ($minimumIdle < 0 || $minimumIdle > $this->size) {
            throw new LogicException();
        }

        $this->minimumIdle = $minimumIdle;

        return $this;
    }

    public function setIdleTimeoutSec(float $idleTimeoutSec): static
    {
        $this->idleTimeoutSec = $idleTimeoutSec;

        return $this;
    }

    public function setMaxLifetimeSec(float $maxLifetimeSec): static
    {
        $this->maxLifetimeSec = $maxLifetimeSec;

        return $this;
    }

    public function setBorrowingTimeoutSec(float $borrowingTimeoutSec): static
    {
        $this->borrowingTimeoutSec = $borrowingTimeoutSec;

        return $this;
    }

    public function setReturningTimeoutSec(float $returningTimeoutSec): static
    {
        $this->returningTimeoutSec = $returningTimeoutSec;

        return $this;
    }

    /**
     * @param  callable(TConnection): bool  $checker
     *
     */
    public function addConnectionChecker(callable $checker): static
    {
        $this->checkers[] = $checker;

        return $this;
    }

    /**
     * @param  KeepaliveCheckerInterface<TConnection>  $keepaliveChecker
     *
     */
    public function addKeepaliveChecker(KeepaliveCheckerInterface $keepaliveChecker): static
    {
        $this->keepaliveCheckers[] = $keepaliveChecker;

        return $this;
    }

    /**
     * @param  TimerTaskInterface<\Allsilaevex\Pool\PoolControlInterface<TConnection>>  $timerTask
     *
     */
    public function addPoolTimerTask(TimerTaskInterface $timerTask): static
    {
        $this->poolTimerTasks[] = $timerTask;

        return $this;
    }

    /**
     *
     * @return PoolInterface<TConnection>
     */
    public function instantiate(string $name = ''): PoolInterface
    {
        if ($name === '') {
            $name = $this->generateName();
        }

        $config = new PoolConfig(
            size: $this->size,
            borrowingTimeoutSec: $this->borrowingTimeoutSec,
            returningTimeoutSec: $this->returningTimeoutSec,
            autoReturn: $this->autoReturn,
            bindToCoroutine: $this->bindToCoroutine,
        );

        /** @var TimerTaskScheduler<\Allsilaevex\Pool\PoolControlInterface<TConnection>> $timerTaskScheduler */
        /** @psalm-suppress InvalidArgument */
        $timerTaskScheduler = new TimerTaskScheduler($this->createPoolTimerTasks());

        $poolItemUpdaterTimerTask = new PoolItemUpdaterTimerTask(
            intervalSec: $this->maxLifetimeSec / 10.0,
            maxLifetimeSec: $this->maxLifetimeSec,
            logger: $this->logger,
            maxItemReservingWaitingTimeSec: $this->maxItemReservingForUpdateWaitingTimeSec,
        );

        $poolItemTimerTasks = array_map(
            fn (KeepaliveCheckerInterface $checker) => new KeepaliveCheckTimerTask($this->logger, $checker),
            $this->keepaliveCheckers,
        );

        /** @var TimerTaskScheduler<PoolItemWrapperInterface<TConnection>> $poolItemTimerTaskScheduler */
        /** @psalm-suppress InvalidArgument */
        $poolItemTimerTaskScheduler = new TimerTaskScheduler([
            $poolItemUpdaterTimerTask,
            ...$poolItemTimerTasks,
        ]);

        $hooks = array_map(fn (callable $checker) => new ConnectionCheckHook($checker, $this->logger), $this->checkers);

        /**
         * @var Pool<TConnection> $pool
         * @psalm-suppress InvalidArgument
         */
        $pool = new Pool(
            name: $name,
            config: $config,
            poolItemWrapperFactory: new PoolItemWrapperFactory(
                factory: $this->factory,
                poolItemTimerTaskScheduler: $poolItemTimerTaskScheduler,
            ),
            logger: $this->logger,
            timerTaskScheduler: $timerTaskScheduler,
            poolItemHookManager: count($hooks) > 0 ? new PoolItemHookManager($hooks) : null,
        );

        return $pool;
    }

    /**
     * @return list<TimerTaskInterface<\Allsilaevex\Pool\PoolControlInterface<TConnection>>>
     */
    protected function createPoolTimerTasks(): array
    {
        /** @var TimerTaskInterface<\Allsilaevex\Pool\PoolControlInterface<TConnection>> $resizerTimerTask */
        // @phpstan-ignore varTag.nativeType
        $resizerTimerTask = new ResizerTimerTask(.1, $this->minimumIdle, $this->idleTimeoutSec, $this->logger);

        /** @var TimerTaskInterface<\Allsilaevex\Pool\PoolControlInterface<TConnection>> $leakDetectionTimerTask */
        // @phpstan-ignore varTag.nativeType
        $leakDetectionTimerTask = new LeakDetectionTimerTask(
            $this->leakDetectionThresholdSec,
            $this->leakDetectionThresholdSec,
            $this->logger,
        );

        return [
            $resizerTimerTask,
            $leakDetectionTimerTask,
            ...$this->poolTimerTasks,
        ];
    }

    /**
     * @return non-empty-string
     */
    protected function generateName(): string
    {
        $factoryRef = new ReflectionClass($this->factory);

        return $factoryRef->getShortName() . '-' . substr(uniqid(), 0, 8);
    }
}
