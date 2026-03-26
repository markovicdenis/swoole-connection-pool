<?php

declare(strict_types=1);

namespace Allsilaevex\ConnectionPool\Hooks;

use Allsilaevex\Pool\Hook\PoolItemHook;
use Allsilaevex\Pool\PoolItemWrapperInterface;
use Allsilaevex\Pool\Hook\PoolItemHookInterface;

use function is_null;

/**
 * @template TItem of object
 * @implements PoolItemHookInterface<TItem>
 */
final readonly class ConnectionResetHook implements PoolItemHookInterface
{
    /**
     * @param  callable(TItem): void  $resetter
     */
    public function __construct(
        protected mixed $resetter,
    ) {
    }

    /**
     * @inheritDoc
     */
    #[\Override]
    public function invoke(PoolItemWrapperInterface $poolItemWrapper): void
    {
        $item = $poolItemWrapper->getItem();
        $resetter = $this->resetter;

        if (!is_null($item)) {
            $resetter($item);
        }
    }

    #[\Override]
    public function getHook(): PoolItemHook
    {
        return PoolItemHook::AFTER_RETURN;
    }
}
