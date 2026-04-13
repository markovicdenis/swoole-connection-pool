<?php

declare(strict_types=1);

namespace Allsilaevex\ConnectionPool\Test\Integration;

use stdClass;
use Allsilaevex\Pool\Pool;
use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use Allsilaevex\Pool\PoolItemFactoryInterface;
use Allsilaevex\Pool\TimerTask\TimerTaskInterface;
use Allsilaevex\ConnectionPool\ConnectionPoolFactory;
use Allsilaevex\ConnectionPool\KeepaliveCheckerInterface;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;

#[AllowMockObjectsWithoutExpectations]
#[CoversClass(ConnectionPoolFactory::class)]
final class ConnectionPoolFactoryTest extends TestCase
{
    public function testInstantiate(): void
    {
        $connection = new stdClass();
        $connection->id = 1;

        $poolItemFactoryInterfaceMock = $this->createMock(PoolItemFactoryInterface::class);
        $poolItemFactoryInterfaceMock->method('create')->willReturn($connection);

        $connectionPoolFactory = new ConnectionPoolFactory(size: 1, factory: $poolItemFactoryInterfaceMock);

        $pool = $connectionPoolFactory->instantiate();

        /** @var stdClass&object{id: int} $connectionFromPool */
        $connectionFromPool = $pool->borrow();

        static::assertEquals($connection->id, $connectionFromPool->id);
    }

    public function testCreatePreservesSubclassType(): void
    {
        $poolItemFactoryInterfaceMock = $this->createMock(PoolItemFactoryInterface::class);

        $subclassFactory = new class(size: 1, factory: $poolItemFactoryInterfaceMock) extends ConnectionPoolFactory {
        };

        $createdFactory = $subclassFactory::create(size: 1, factory: $poolItemFactoryInterfaceMock)
            ->setAutoReturn(false);

        static::assertSame($subclassFactory::class, $createdFactory::class);
    }

    public function testSetMinimumIdleAllowsZero(): void
    {
        $poolItemFactoryInterfaceMock = $this->createMock(PoolItemFactoryInterface::class);

        $connectionPoolFactory = ConnectionPoolFactory::create(size: 2, factory: $poolItemFactoryInterfaceMock);

        static::assertSame($connectionPoolFactory, $connectionPoolFactory->setMinimumIdle(0));
    }

    public function testCustomPoolTimerTaskRunsOnInstantiate(): void
    {
        $poolItemFactoryInterfaceMock = $this->createMock(PoolItemFactoryInterface::class);

        $timerTask = new class() implements TimerTaskInterface {
            /** @var list<string> */
            public array $runnerNames = [];

            #[\Override]
            public function run(int $timerId, mixed $runnerRef): void
            {
                $runner = $runnerRef->get();

                if ($runner !== null) {
                    $this->runnerNames[] = $runner->getName();
                }
            }

            #[\Override]
            public function getIntervalSec(): float
            {
                return 60.0;
            }
        };

        ConnectionPoolFactory::create(size: 1, factory: $poolItemFactoryInterfaceMock)
            ->addPoolTimerTask($timerTask)
            ->instantiate(name: 'custom-pool');

        static::assertSame(['custom-pool'], $timerTask->runnerNames);
    }

    public function testMaxLifetimeRecreatesIdleConnection(): void
    {
        $factory = new /**
         * @implements PoolItemFactoryInterface<stdClass&object{id: int}>
         */ class() implements PoolItemFactoryInterface {
            private int $_nextId = 0;

            #[\Override]
            public function create(): mixed
            {
                /** @var stdClass&object{id: int} $connection */
                $connection = new stdClass();
                $connection->id = ++$this->_nextId;

                return $connection;
            }
        };

        $connectionPoolFactory = ConnectionPoolFactory::create(size: 1, factory: $factory)
            ->setMaxLifetimeSec(.02);

        $pool = $connectionPoolFactory->instantiate();

        /** @var stdClass&object{id: int} $firstConnection */
        $firstConnection = $pool->borrow();
        $firstId = $firstConnection->id;
        $pool->return($firstConnection);

        \Swoole\Coroutine::sleep(.05);

        /** @var stdClass&object{id: int} $secondConnection */
        $secondConnection = $pool->borrow();

        static::assertNotSame($firstId, $secondConnection->id);

        $pool->return($secondConnection);
    }

    public function testKeepaliveCheckerRecreatesIdleConnection(): void
    {
        $factory = new /**
         * @implements PoolItemFactoryInterface<stdClass&object{id: int}>
         */ class() implements PoolItemFactoryInterface {
            private int $_nextId = 0;

            #[\Override]
            public function create(): mixed
            {
                /** @var stdClass&object{id: int} $connection */
                $connection = new stdClass();
                $connection->id = ++$this->_nextId;

                return $connection;
            }
        };

        /** @var stdClass&object{staleConnectionId: int|null} $state */
        $state = new stdClass();
        $state->staleConnectionId = null;

        $checker = new /**
         * @implements KeepaliveCheckerInterface<stdClass&object{id: int}>
         */ class($state) implements KeepaliveCheckerInterface {
            public function __construct(
                private stdClass $_state,
            ) {
            }

            #[\Override]
            public function check(mixed $connection): bool
            {
                return $connection instanceof stdClass
                    && ($this->_state->staleConnectionId === null || $connection->id !== $this->_state->staleConnectionId);
            }

            #[\Override]
            public function getIntervalSec(): float
            {
                return .01;
            }
        };

        $connectionPoolFactory = ConnectionPoolFactory::create(size: 1, factory: $factory)
            ->addKeepaliveChecker($checker);

        $pool = $connectionPoolFactory->instantiate();

        /** @var stdClass&object{id: int} $firstConnection */
        $firstConnection = $pool->borrow();
        $firstId = $firstConnection->id;
        $state->staleConnectionId = $firstId;
        $pool->return($firstConnection);

        $replacementId = $firstId;

        for ($attempt = 0; $attempt < 10 && $replacementId === $firstId; $attempt++) {
            \Swoole\Coroutine::sleep(.02);

            /** @var stdClass&object{id: int} $secondConnection */
            $secondConnection = $pool->borrow();
            $replacementId = $secondConnection->id;
            $pool->return($secondConnection);
        }

        static::assertNotSame($firstId, $replacementId);
    }

    public function testResizerShrinksIdlePoolAfterTimeout(): void
    {
        $factory = new /**
         * @implements PoolItemFactoryInterface<stdClass&object{id: int}>
         */ class() implements PoolItemFactoryInterface {
            private int $_nextId = 0;

            #[\Override]
            public function create(): mixed
            {
                /** @var stdClass&object{id: int} $connection */
                $connection = new stdClass();
                $connection->id = ++$this->_nextId;

                return $connection;
            }
        };

        $connectionPoolFactory = ConnectionPoolFactory::create(size: 2, factory: $factory)
            ->setAutoReturn(false)
            ->setBindToCoroutine(false)
            ->setMinimumIdle(1)
            ->setIdleTimeoutSec(.05);

        $pool = $connectionPoolFactory->instantiate();

        static::assertInstanceOf(Pool::class, $pool);

        /** @var Pool<stdClass&object{id: int}> $pool */
        $firstConnection = $pool->borrow();
        $secondConnection = $pool->borrow();

        $pool->return($firstConnection);
        $pool->return($secondConnection);

        static::assertGreaterThan(1, $pool->getCurrentSize());

        for ($attempt = 0; $attempt < 20; $attempt++) {
            if ($pool->getCurrentSize() < 2) {
                break;
            }

            \Swoole\Coroutine::sleep(.02);
        }

        static::assertEquals(1, $pool->getCurrentSize());
    }

    public function testResizerDoesNotPrewarmAndDrainsPoolWhenMinimumIdleIsZero(): void
    {
        $factory = new /**
         * @implements PoolItemFactoryInterface<stdClass&object{id: int}>
         */ class() implements PoolItemFactoryInterface {
            private int $_nextId = 0;

            #[\Override]
            public function create(): mixed
            {
                /** @var stdClass&object{id: int} $connection */
                $connection = new stdClass();
                $connection->id = ++$this->_nextId;

                return $connection;
            }
        };

        $connectionPoolFactory = ConnectionPoolFactory::create(size: 3, factory: $factory)
            ->setAutoReturn(false)
            ->setBindToCoroutine(false)
            ->setMinimumIdle(0)
            ->setIdleTimeoutSec(.05);

        $pool = $connectionPoolFactory->instantiate();

        static::assertInstanceOf(Pool::class, $pool);

        /** @var Pool<stdClass&object{id: int}> $pool */
        static::assertSame(0, $pool->getCurrentSize());
        static::assertSame(0, $pool->getIdleCount());

        \Swoole\Coroutine::sleep(.12);

        static::assertSame(0, $pool->getCurrentSize());
        static::assertSame(0, $pool->getIdleCount());

        /** @var stdClass&object{id: int} $connection */
        $connection = $pool->borrow();

        static::assertSame(1, $pool->getCurrentSize());
        static::assertSame(0, $pool->getIdleCount());

        $pool->return($connection);

        static::assertSame(1, $pool->getCurrentSize());
        static::assertSame(1, $pool->getIdleCount());

        for ($attempt = 0; $attempt < 20; $attempt++) {
            if ($pool->getCurrentSize() === 0 && $pool->getIdleCount() === 0) {
                break;
            }

            \Swoole\Coroutine::sleep(.02);
        }

        static::assertSame(0, $pool->getCurrentSize());
        static::assertSame(0, $pool->getIdleCount());
    }
}
